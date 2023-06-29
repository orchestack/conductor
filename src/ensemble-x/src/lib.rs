use std::sync::Arc;

use async_trait::async_trait;
use catalog::{edit::Edit, Catalog};
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::TableProvider,
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
    physical_plan::{RecordBatchStream, SendableRecordBatchStream, Statistics},
    prelude::Expr,
};
use deltalake::writer::DeltaWriter;
use deltalake::{
    operations::create::CreateBuilder, storage::DeltaObjectStore, writer::RecordBatchWriter,
    DeltaTable, DeltaTableBuilder, SchemaDataType, SchemaField,
};
use futures::{Stream, StreamExt};
use object_store::{path::Path, prefix::PrefixStore, ObjectStore as ObjectStoreTrait};
use serde_json::json;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::trace;
use url::Url;

use crate::storage::ObjectStore;

pub mod storage;

#[derive(Debug, Error)]
pub enum Error {
    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("delta table error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),
    #[error("catalog error: {0}")]
    CatalogError(#[from] catalog::Error),
    #[error("ensemble error: {0}")]
    Error(String),
}

const METADATA_TABLE_UUID: &str = "orchestack.table-uuid";
const METADATA_COLUMN_UID: &str = "orchestack.column-uid";

pub struct EnsembleX {
    storage: ObjectStore,
    catalog: Catalog,
    pending_actions: Vec<Action>,
}

pub struct TableX {
    inner: Mutex<DeltaTable>,
}
enum Action {
    CreateTable(CreateBuilder),
    DropTable { namespace: String, name: String },
}

const CATALOG_PATH: &str = "_conductor_catalog.json";

impl EnsembleX {
    pub async fn new(storage: ObjectStore) -> Result<Self, Error> {
        let catalog = match storage.get(&Path::parse(CATALOG_PATH).unwrap()).await {
            Ok(get_result) => Ok(serde_json::from_slice(&get_result.bytes().await?.slice(..))
                .map_err(|e| Error::Error(e.to_string()))?),
            Err(object_store::Error::NotFound { .. }) => Ok(Catalog::default()),
            Err(e) => Err(e),
        }?;

        Ok(Self {
            storage: storage.clone(),
            catalog,
            pending_actions: vec![],
        })
    }

    pub fn catalog(&self) -> Result<Catalog, Error> {
        Ok(self.catalog.clone())
    }

    pub async fn table(&self, namespace: &str, name: &str) -> Result<Arc<TableX>, Error> {
        trace!(?namespace, ?name, "table");
        let (store, location) = self.store_for_table(namespace, name);

        Ok(Arc::new(TableX {
            inner: Mutex::new(
                DeltaTableBuilder::from_uri(location.clone())
                    .with_storage_backend(store, location)
                    .load()
                    .await?,
            ),
        }))
    }

    pub async fn apply(&mut self, edit: &Edit) -> Result<(), Error> {
        match edit {
            Edit::CreateTable(table) => {
                let delta_columns = table
                    .columns
                    .iter()
                    .map(|c| {
                        let col_meta = [(METADATA_COLUMN_UID.to_string(), json!(c.uid))].into();

                        SchemaField::new(c.name.to_string(), map_type(&c.data_type), true, col_meta)
                    })
                    .collect::<Vec<_>>();

                let mut table_metadata = serde_json::Map::new();
                table_metadata.insert(METADATA_TABLE_UUID.to_string(), json!(table.uuid));

                let (store, location) = self.store_for_table(&table.namespace, &table.name);
                let delta_storage = Arc::new(DeltaObjectStore::new(store, location));

                let create_builder = CreateBuilder::new()
                    .with_table_name(table.name.clone())
                    .with_columns(delta_columns)
                    .with_metadata(table_metadata)
                    .with_object_store(delta_storage);

                self.catalog.apply(edit)?;

                self.pending_actions
                    .push(Action::CreateTable(create_builder));
            }
            Edit::DropTable(table) => {
                self.catalog.apply(edit)?;

                self.pending_actions.push(Action::DropTable {
                    namespace: table.namespace.clone(),
                    name: table.name.clone(),
                });
            }
            edit @ Edit::CreateNamespace { .. }
            | edit @ Edit::ReplaceHttpHandler(_)
            | edit @ Edit::DropHttpHandler(_)
            | edit @ Edit::ReplaceAuthenticationPolicy(_)
            | edit @ Edit::DropAuthenticationPolicy(_)
            | edit @ Edit::ReplaceAuthorizationPolicy(_)
            | edit @ Edit::DropAuthorizationPolicy(_) => self.catalog.apply(edit)?,
        }

        Ok(())
    }

    pub async fn commit(&mut self) -> Result<(), Error> {
        let actions = std::mem::take(&mut self.pending_actions);

        for action in actions.into_iter() {
            match action {
                Action::CreateTable(create_builder) => {
                    create_builder.await?;
                }
                Action::DropTable { namespace, name } => {
                    let prefix = self.store_for_namespace(&namespace);
                    let mut lst = prefix
                        .list(Some(&object_store::path::Path::parse(&name).unwrap()))
                        .await?;
                    while let Some(e) = lst.next().await {
                        let meta = e?;

                        prefix.delete(&meta.location).await?;
                    }

                    // TODO: Cleanup prefixes (folders on local filesystem).
                    //  These aren't present in the list call above, but they
                    //  do prevent folder deletion on the filesystem.
                }
            }
        }

        let catalog_json_bytes =
            serde_json::to_vec(&self.catalog).map_err(|e| Error::Error(e.to_string()))?;

        self.storage
            .put(
                &Path::parse(CATALOG_PATH).unwrap(),
                catalog_json_bytes.into(),
            )
            .await?;

        Ok(())
    }

    fn store_for_namespace(&self, namespace: &str) -> Arc<PrefixStore<storage::ObjectStore>> {
        Arc::new(PrefixStore::new(
            self.storage.clone(),
            object_store::path::Path::parse(namespace).unwrap(),
        ))
    }

    fn store_for_table(
        &self,
        namespace: &str,
        name: &str,
    ) -> (Arc<PrefixStore<storage::ObjectStore>>, Url) {
        trace!(?namespace, ?name, "store_for_table");
        let mut location = self.storage.location().clone();
        location
            .path_segments_mut()
            .unwrap()
            .push(namespace)
            .push(name);

        let store = Arc::new(PrefixStore::new(
            self.storage.clone(),
            object_store::path::Path::parse(namespace)
                .unwrap()
                .child(object_store::path::PathPart::parse(name).unwrap()),
        ));

        (store, location)
    }
}

fn map_type(dt: &sqlparser::ast::DataType) -> SchemaDataType {
    match dt {
        sqlparser::ast::DataType::Integer(_) => SchemaDataType::primitive("integer".to_string()),
        sqlparser::ast::DataType::Text => SchemaDataType::primitive("string".to_string()),
        _ => todo!(),
    }
}

impl TableX {
    pub async fn write(&self, input: SendableRecordBatchStream) -> Result<(), Error> {
        let mut table = self.inner.lock().await;
        let mut writer = RecordBatchWriter::for_table(&table)?;
        let mut schema_adapter = SchemaAdapterStream::new(input, writer.arrow_schema());

        while let Some(batch) = schema_adapter.next().await {
            writer.write(batch.unwrap()).await?;
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

struct SchemaAdapterStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl SchemaAdapterStream {
    pub fn new(input: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        Self { input, schema }
    }

    fn adapt_batch(&self, batch: RecordBatch) -> datafusion::error::Result<RecordBatch> {
        let mut columns = vec![];
        let schema = batch.schema();

        for field in self.schema.fields() {
            match schema.index_of(field.name()) {
                Ok(field_ix) => columns.push(batch.column(field_ix).clone()),
                Err(_) => {
                    columns.push(datafusion::arrow::array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    ));
                }
            }
        }

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl RecordBatchStream for SchemaAdapterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SchemaAdapterStream {
    type Item = datafusion::error::Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.as_mut().poll_next(cx).map(|maybe_result| {
            maybe_result.map(|batch| batch.and_then(|batch| self.adapt_batch(batch)))
        })
    }
}
