use std::path::PathBuf;

use catalog::{Catalog, Namespace};
use deltalake::{operations::create::CreateBuilder, SchemaDataType, SchemaField};
use sqlparser::ast::Statement;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("ensemble error: {0}")]
    Error(String),
    #[error("delta table error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),
}

pub struct EnsembleX {
    deltalake_path: PathBuf,
}

impl EnsembleX {
    pub fn with_deltalake_path(path: PathBuf) -> Self {
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

    pub async fn apply(&self, stmt: &Statement) -> Result<(), Error> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                let delta_columns = columns
                    .iter()
                    .map(|c| {
                        let name = &c.name.value;
                        let data_type = c.data_type.to_string();

                        // TODO: column uid in metadata
                        SchemaField::new(
                            name.to_string(),
                            SchemaDataType::primitive(data_type),
                            true,
                            Default::default(),
                        )
                    })
                    .collect::<Vec<_>>();

                // TODO: uuid in metadata
                let _create = CreateBuilder::new()
                    .with_table_name(name.0[0].value.clone())
                    .with_columns(delta_columns)
                    .with_location(self.deltalake_path.join(name.to_string()).to_str().unwrap())
                    .await?;
            }
            _ => {
                todo!()
            }
        }

        Ok(())
    }
}
