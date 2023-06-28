use std::sync::Arc;

use datafusion::{
    arrow::{datatypes::DataType, record_batch::RecordBatch},
    common::DFSchema,
    config::ConfigOptions,
    error::Result as DFResult,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    physical_plan::ColumnarValue,
    scalar::ScalarValue,
    sql::{
        planner::{ContextProvider, PlannerContext, SqlToRel},
        TableReference,
    },
};

use crate::AuthorizationPolicy;

#[derive(Debug, Default)]
pub struct AuthorizationExprContext {
    config: ConfigOptions,
}

impl ContextProvider for AuthorizationExprContext {
    fn get_table_provider(&self, _name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        todo!()
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.config
    }
}

#[derive(Debug, Default)]
pub struct AuthEval {}

impl AuthEval {
    pub fn eval_policy(&self, policy: &AuthorizationPolicy) -> bool {
        let auth_context = AuthorizationExprContext::default();
        let mut planner_context = PlannerContext::default();
        let sql_to_rel = SqlToRel::new(&auth_context);
        let df_schema = DFSchema::empty();
        let schema: Arc<datafusion::arrow::datatypes::Schema> = Arc::new(df_schema.clone().into());
        let rel_expr = sql_to_rel
            .sql_to_expr(
                policy.permissive_expr.clone(),
                &df_schema,
                &mut planner_context,
            )
            .unwrap();
        let phys_expr =
            create_physical_expr(&rel_expr, &df_schema, &schema, &ExecutionProps::default())
                .unwrap();
        let eval = phys_expr.evaluate(&RecordBatch::new_empty(schema)).unwrap();

        matches!(
            eval,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        )
    }
}
