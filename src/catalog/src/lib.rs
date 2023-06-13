use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod diff;
pub mod edit;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    pub namespaces: HashMap<String, Namespace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

impl Namespace {
    fn get_table_by_uuid(&self, uuid: uuid::Uuid) -> Option<&Table> {
        self.tables.values().find(|t| t.uuid == uuid)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub namespace: String,
    pub uuid: uuid::Uuid,
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn get_column_by_uid(&self, uid: u32) -> Option<&Column> {
        self.columns.iter().find(|c| c.uid == uid)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub uid: u32,
    pub name: String,
    pub data_type: sqlparser::ast::DataType,
}
