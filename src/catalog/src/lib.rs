use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod auth;
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
    pub authentication_policies: HashMap<String, AuthenticationPolicy>,
    pub authorization_policies: HashMap<String, AuthorizationPolicy>,
}

impl Namespace {
    fn get_table_by_uuid(&self, uuid: uuid::Uuid) -> Option<&Table> {
        self.tables.values().find(|t| t.uuid == uuid)
    }

    fn get_http_handler_by_name(&self, name: &str) -> Option<&HttpHandler> {
        self.http_handlers.get(name)
    }

    fn get_authentication_policy_by_name(&self, name: &str) -> Option<&AuthenticationPolicy> {
        self.authentication_policies.get(name)
    }

    fn get_authorization_policy_by_name(&self, name: &str) -> Option<&AuthorizationPolicy> {
        self.authorization_policies.get(name)
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
    pub policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthenticationPolicyType {
    Anonymous(),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationPolicy {
    pub namespace: String,
    pub name: String,
    pub typ: AuthenticationPolicyType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationPolicy {
    pub namespace: String,
    pub name: String,
    pub permissive_expr: sqlparser::ast::Expr,
}
