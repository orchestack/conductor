use std::path::PathBuf;

use catalog::{edit::Edit, Catalog};
use deltalake::{operations::create::CreateBuilder, SchemaDataType, SchemaField};
use serde_json::json;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("delta table error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),
    #[error("ensemble error: {0}")]
    Error(String),
}

const METADATA_TABLE_UUID: &str = "conductor-table-uuid";
const METADATA_COLUMN_UID: &str = "conductor-column-uid";

pub struct EnsembleX {
    pub deltalake_path: PathBuf,
    pub catalog: Catalog,
    pending_actions: Vec<Action>,
}

enum Action {
    CreateTable(CreateBuilder),
}

impl EnsembleX {
    pub async fn with_path(path: PathBuf) -> Result<Self, Error> {
        if !path.join("_conductor_catalog.json").exists() {
            let catalog = Catalog {
                root: catalog::Namespace {
                    name: "northwind".to_string(),
                    tables: Default::default(),
                },
            };

            serde_json::to_writer(
                std::fs::File::create(path.join("_conductor_catalog.json"))?,
                &catalog,
            )
            .map_err(|e| Error::Error(e.to_string()))?;
        }

        let catalog =
            serde_json::from_reader(std::fs::File::open(path.join("_conductor_catalog.json"))?)
                .map_err(|e| Error::Error(e.to_string()))?;

        Ok(Self {
            deltalake_path: path,
            catalog,
            pending_actions: vec![],
        })
    }

    pub fn catalog(&self) -> Result<Catalog, Error> {
        Ok(self.catalog.clone())
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

                let create_builder = CreateBuilder::new()
                    .with_table_name(table.name.clone())
                    .with_columns(delta_columns)
                    .with_metadata(table_metadata)
                    .with_location(
                        self.deltalake_path
                            .join(table.namespace.clone())
                            .join(table.name.clone())
                            .to_str()
                            .unwrap(),
                    );

                self.catalog
                    .root
                    .tables
                    .insert(table.name.clone(), table.clone());

                self.pending_actions
                    .push(Action::CreateTable(create_builder));
            }
            _ => {
                todo!()
            }
        }

        Ok(())
    }

    pub async fn commit(&mut self) -> Result<(), Error> {
        for action in self.pending_actions.drain(..) {
            match action {
                Action::CreateTable(create_builder) => {
                    create_builder.await?;
                }
            }
        }

        serde_json::to_writer(
            std::fs::File::create(self.deltalake_path.join("_conductor_catalog.json"))?,
            &self.catalog,
        )
        .map_err(|e| Error::Error(e.to_string()))?;

        Ok(())
    }
}

fn map_type(dt: &sqlparser::ast::DataType) -> SchemaDataType {
    match dt {
        sqlparser::ast::DataType::Integer(_) => SchemaDataType::primitive("integer".to_string()),
        sqlparser::ast::DataType::Text => SchemaDataType::primitive("string".to_string()),
        _ => todo!(),
    }
}
