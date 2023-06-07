use std::path::PathBuf;

use catalog::{edit::Edit, Catalog, Namespace};
use deltalake::{operations::create::CreateBuilder, SchemaDataType, SchemaField};
use serde_json::json;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("ensemble error: {0}")]
    Error(String),
    #[error("delta table error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),
}

const METADATA_TABLE_UUID: &str = "conductor-table-uuid";
const METADATA_COLUMN_UID: &str = "conductor-column-uid";

pub struct EnsembleX {
    deltalake_path: PathBuf,
}

impl EnsembleX {
    pub fn with_path(path: PathBuf) -> Self {
        Self {
            deltalake_path: path,
        }
    }

    pub fn catalog(&self) -> Result<Catalog, Error> {
        println!("{}", self.deltalake_path.display());

        let catalog = Catalog {
            root: Namespace {
                name: "northwind".to_string(),
                tables: Default::default(),
            },
        };

        Ok(catalog)
    }

    pub async fn apply(&self, edit: &Edit) -> Result<(), Error> {
        match edit {
            Edit::CreateTable(table) => {
                let delta_columns = table
                    .columns
                    .iter()
                    .map(|c| {
                        let data_type = c.data_type.to_string();
                        let col_meta = [(METADATA_COLUMN_UID.to_string(), json!(c.uid))].into();

                        SchemaField::new(
                            c.name.to_string(),
                            SchemaDataType::primitive(data_type),
                            true,
                            col_meta,
                        )
                    })
                    .collect::<Vec<_>>();

                let mut table_metadata = serde_json::Map::new();
                table_metadata.insert(METADATA_TABLE_UUID.to_string(), json!(table.uuid));

                let _create = CreateBuilder::new()
                    .with_table_name(table.name.clone())
                    .with_columns(delta_columns)
                    .with_metadata(table_metadata)
                    .with_location(
                        self.deltalake_path
                            .join(table.namespace.clone())
                            .join(table.name.clone())
                            .to_str()
                            .unwrap(),
                    )
                    .await?;
            }
            _ => {
                todo!()
            }
        }

        Ok(())
    }
}
