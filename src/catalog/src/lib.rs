use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod diff;
pub mod edit;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    pub namespaces: HashMap<String, Namespace>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub name: String,
    pub tables: HashMap<String, Table>,
    pub http_handlers: HashMap<String, HttpHandler>,
}

impl Namespace {
    fn get_table_by_uuid(&self, uuid: uuid::Uuid) -> Option<&Table> {
        self.tables.values().find(|t| t.uuid == uuid)
    }

    fn get_http_handler_by_name(&self, name: &str) -> Option<&HttpHandler> {
        self.http_handlers.get(name)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpHandler {
    pub namespace: String,
    pub name: String,
    pub body: String,
}
