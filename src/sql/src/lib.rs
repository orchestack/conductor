use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    catalog::schema::{MemorySchemaProvider, SchemaProvider},
    datasource::TableProvider,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableType},
    physical_plan::{ExecutionPlan, Statistics},
    prelude::{DataFrame, Expr, SessionConfig},
    sql::parser::Statement as DFStatement,
};
use deltalake::{
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaTable,
};
use ensemble_x::EnsembleX;
use futures::lock::Mutex;
use thiserror::Error;

mod parser;

#[derive(Error, Debug)]
pub enum Error {
    #[error("deltalake error: {0}")]
    DeltaLakeError(#[from] deltalake::DeltaTableError),
    #[error("ensemble error: {0}")]
    EnsembleError(#[from] ensemble_x::Error),
    #[error("datafusion error: {0}")]
    DFError(#[from] datafusion::error::DataFusionError),
    #[error("sql parser error: {0}")]
    ParserError(#[from] sqlparser::parser::ParserError),
    #[error("sql tokenizer error: {0}")]
    TokenizerError(#[from] sqlparser::tokenizer::TokenizerError),
    #[error("sql error: {0}")]
    Error(String),
}

pub struct SqlSession {
    state: SessionState,
    tables: HashMap<String, Arc<TableX>>,
}

impl SqlSession {
    #[allow(clippy::new_without_default)]
    pub async fn new(ensemble: EnsembleX) -> Result<Self, Error> {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("conductor", "public")
            .with_create_default_catalog_and_schema(true);
        let state = SessionState::with_config_rt(config, Arc::new(RuntimeEnv::default()));

        let mut tables = HashMap::new();
        let catalog = ensemble.catalog()?;
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        for table in catalog.root.tables.values() {
            let x_table = Arc::new(TableX {
                inner: Mutex::new(
                    deltalake::open_table(
                        ensemble
                            .deltalake_path
                            .join(&catalog.root.name)
                            .join(&table.name)
                            .to_str()
                            .unwrap(),
                    )
                    .await?,
                ),
            });

            tables.insert(table.name.clone(), x_table.clone());
            schema_provider.register_table(table.name.clone(), x_table)?;
        }

        state
            .catalog_list()
            .catalog("conductor")
            .unwrap()
            .register_schema(&catalog.root.name, schema_provider)?;

        Ok(SqlSession { state, tables })
    }

    pub fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<(), Error> {
        self.state
            .catalog_list()
            .catalog("default")
            .unwrap()
            .register_schema(name, schema)?;

        Ok(())
    }

    pub async fn execute(&mut self, sql: &str) -> Result<Vec<RecordBatch>, Error> {
        let mut parser = parser::SqlParser::new(sql)?;
        let mut statements = parser.parse_sql()?;
        assert_eq!(statements.len(), 1, "multiple statements not supported yet");

        let parser::Statement::Statement(stmt) = statements.pop_front().unwrap();

        let plan = self
            .state
            .statement_to_plan(DFStatement::Statement(Box::new(stmt)))
            .await?;

        match plan {
            plan @ LogicalPlan::Projection(_) => {
                Ok(DataFrame::new(self.state.clone(), plan).collect().await?)
            }
            plan @ LogicalPlan::Dml(_) => {
                match plan.clone() {
                    LogicalPlan::Dml(dml_stmt) => {
                        match dml_stmt.op {
                            datafusion::logical_expr::WriteOp::Insert => {
                                // Collect the input plan.
                                let input =
                                    DataFrame::new(self.state.clone(), (*dml_stmt.input).clone())
                                        .collect()
                                        .await?;

                                // Get the delta table handle.
                                // TODO: Qualify table names correctly.
                                let table = self
                                    .tables
                                    .get_mut(dml_stmt.table_name.table())
                                    .ok_or(Error::Error(format!(
                                        "table not found: {}",
                                        dml_stmt.table_name
                                    )))?;

                                table.write(input).await?;

                                return Ok(vec![]);
                            }
                            _ => Err(Error::Error(format!(
                                "unsupported logical plan: {:?}",
                                plan
                            )))?,
                        }
                    }
                    _ => Err(Error::Error(format!(
                        "unsupported logical plan: {:?}",
                        plan
                    )))?,
                }

                Ok(DataFrame::new(self.state.clone(), plan).collect().await?)
            }
            plan => Err(Error::Error(format!(
                "unsupported logical plan: {:?}",
                plan
            ))),
        }
    }
}

struct TableX {
    inner: Mutex<DeltaTable>,
}

impl TableX {
    async fn write(&self, input: Vec<RecordBatch>) -> Result<(), Error> {
        let mut table = self.inner.lock().await;
        let mut writer = RecordBatchWriter::for_table(&table)?;
        for batch in input {
            writer
                .write(batch.with_schema(writer.arrow_schema()).unwrap())
                .await?;
        }
        writer.flush_and_commit(&mut table).await?;

        Ok(())
    }
}

#[async_trait]
impl TableProvider for TableX {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        futures::executor::block_on(async {
            let table = self.inner.lock().await;
            table.get_state().arrow_schema().unwrap()
        })
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let table = self.inner.lock().await;
        table.scan(state, projection, filters, limit).await
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> datafusion::error::Result<TableProviderFilterPushDown> {
        futures::executor::block_on(async {
            let table = self.inner.lock().await;

            #[allow(deprecated)]
            table.supports_filter_pushdown(filter)
        })
    }

    fn statistics(&self) -> Option<Statistics> {
        futures::executor::block_on(async {
            let table = self.inner.lock().await;
            table.statistics()
        })
    }
}
