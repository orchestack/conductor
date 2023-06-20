use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::TableType,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    prelude::Expr,
};

pub(crate) struct HttpHandlerInput {
    schema: SchemaRef,
    body: String,
}

impl HttpHandlerInput {
    pub(crate) fn new(body: String) -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)])),
            body,
        }
    }
}

#[async_trait]
impl TableProvider for HttpHandlerInput {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let body_array = StringArray::from(vec![self.body.clone()]);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(body_array)]).unwrap();

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema.clone(),
            None,
        )?))
    }
}
