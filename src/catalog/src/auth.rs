use std::sync::Arc;

use datafusion::{
    arrow::{
        array::BooleanArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
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
        let placeholder_a = Field::new("placeholder_a", DataType::Boolean, true);
        let schema = Arc::new(Schema::new(vec![placeholder_a]));
        let df_schema = DFSchema::try_from((*schema).clone()).unwrap();
        let placeholder_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BooleanArray::from(vec![None]))],
        )
        .unwrap();

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
        let eval = phys_expr.evaluate(&placeholder_batch).unwrap();

        match eval {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => true,
            ColumnarValue::Array(array_ref) => {
                if let Some(array_ref) = array_ref.as_any().downcast_ref::<BooleanArray>() {
                    array_ref.value(0)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_eval() {
        use super::AuthEval;

        let auth_eval = AuthEval::default();

        // Test always true policy.
        assert!(auth_eval.eval_policy(&policy_for_expr("true")));

        // Test always false policy.
        assert!(!auth_eval.eval_policy(&policy_for_expr("false")));

        // Test a non-scalar policy.
        assert!(auth_eval.eval_policy(&policy_for_expr("1 = 1")));

        // Test a bad policy.
        assert!(!auth_eval.eval_policy(&policy_for_expr("1 + 1")));
    }

    fn policy_for_expr(expr: &str) -> AuthorizationPolicy {
        AuthorizationPolicy {
            permissive_expr: parse_expr(expr),
            namespace: "a namespace".to_string(),
            name: "policy name".to_string(),
        }
    }

    fn parse_expr(expr: &str) -> sqlparser::ast::Expr {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(expr).unwrap();
        parser.parse_expr().unwrap()
    }
}
