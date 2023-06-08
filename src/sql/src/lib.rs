use std::sync::Arc;

use datafusion::{
    arrow::record_batch::RecordBatch,
    catalog::schema::{MemorySchemaProvider, SchemaProvider},
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SessionConfig},
    sql::parser::Statement as DFStatement,
};
// use deltalake::delta_datafusion::TableProvider;
use ensemble_x::EnsembleX;
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
}

impl SqlSession {
    #[allow(clippy::new_without_default)]
    pub async fn new(ensemble: EnsembleX) -> Result<Self, Error> {
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("conductor", "public")
            .with_create_default_catalog_and_schema(true);
        let state = SessionState::with_config_rt(config, Arc::new(RuntimeEnv::default()));

        let catalog = ensemble.catalog()?;
        let schema_provider = Arc::new(MemorySchemaProvider::new());

        // let table = deltalake::open_table(&ensemble.deltalake_path)?;
        let table_name = "foo";
        let table = Arc::new(
            deltalake::open_table(
                ensemble
                    .deltalake_path
                    .join(&catalog.root.name)
                    .join(table_name)
                    .to_str()
                    .unwrap(),
            )
            .await?,
        );
        schema_provider.register_table(table_name.to_string(), table)?;

        state
            .catalog_list()
            .catalog("conductor")
            .unwrap()
            .register_schema(&catalog.root.name, schema_provider)?;

        Ok(SqlSession { state })
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

    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>, Error> {
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
            plan => Err(Error::Error(format!(
                "unsupported logical plan: {:?}",
                plan
            ))),
        }
    }
}
