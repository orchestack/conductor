use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::record_batch::RecordBatch,
    catalog::schema::{MemorySchemaProvider, SchemaProvider},
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SessionConfig},
    sql::parser::Statement as DFStatement,
};
use ensemble_x::{EnsembleX, TableX};

use thiserror::Error;

pub mod parser;

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
    pub state: SessionState,
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

        for ns in catalog.namespaces.values() {
            let schema_provider = Arc::new(MemorySchemaProvider::new());
            for table in ns.tables.values() {
                let x_table = ensemble.table(&table.namespace, &table.name).await?;

                tables.insert(table.name.clone(), x_table.clone());
                schema_provider.register_table(table.name.clone(), x_table)?;
            }

            state
                .catalog_list()
                .catalog("conductor")
                .unwrap()
                .register_schema(&ns.name, schema_provider)?;
        }

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
        let stmt;

        {
            let mut parser = parser::SqlParser::new(sql)?;
            let mut statements = parser.parse_sql()?;
            assert_eq!(statements.len(), 1, "multiple statements not supported yet");

            parser::Statement::Statement(stmt) = statements.pop_front().unwrap();
        }

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
                                    DataFrame::new(self.state.clone(), (*dml_stmt.input).clone());

                                // Get the delta table handle.
                                // TODO: Qualify table names correctly.
                                let table = self
                                    .tables
                                    .get_mut(dml_stmt.table_name.table())
                                    .ok_or(Error::Error(format!(
                                        "table not found: {}",
                                        dml_stmt.table_name
                                    )))?;

                                table.write(input.execute_stream().await?).await?;

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
