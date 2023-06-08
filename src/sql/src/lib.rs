use std::sync::Arc;

use datafusion::{
    arrow::record_batch::RecordBatch,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SessionConfig},
    sql::parser::Statement as DFStatement,
};
use thiserror::Error;

mod parser;

#[derive(Error, Debug)]
pub enum Error {
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
    pub fn new() -> Self {
        let config = SessionConfig::new().with_information_schema(true);
        let state = SessionState::with_config_rt(config, Arc::new(RuntimeEnv::default()));

        SqlSession { state }
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
