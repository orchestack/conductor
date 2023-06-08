use std::sync::Arc;

use datafusion::{
    arrow::record_batch::RecordBatch,
    execution::{context::SessionState, runtime_env::RuntimeEnv},
    logical_expr::LogicalPlan,
    prelude::{DataFrame, SessionConfig},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("datafusion error: {0}")]
    DFError(#[from] datafusion::error::DataFusionError),
    #[error("sql error: {0}")]
    Error(String),
}

pub struct SqlSession {
    state: SessionState,
}

impl SqlSession {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let config = SessionConfig::new();
        let state = SessionState::with_config_rt(config, Arc::new(RuntimeEnv::default()));

        SqlSession { state }
    }

    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>, Error> {
        let plan = self.state.create_logical_plan(sql).await?;

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
