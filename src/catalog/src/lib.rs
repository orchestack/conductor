use std::collections::HashMap;

use edit::Edit;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod auth;
pub mod diff;
pub mod edit;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid edit: {0}")]
    InvalidEdit(String),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    pub namespaces: HashMap<String, Namespace>,
}

impl Catalog {
    pub fn apply(&mut self, edit: &Edit) -> Result<()> {
        match edit {
            Edit::CreateNamespace { name } => {
                self.namespaces.insert(
                    name.clone(),
                    Namespace {
                        name: name.clone(),
                        ..Default::default()
                    },
                );
            }
            Edit::CreateTable(table) => {
                self.namespaces
                    .get_mut(table.namespace.as_str())
                    .unwrap()
                    .tables
                    .insert(table.name.clone(), table.clone());
            }
            Edit::DropTable(table) => {
                self.namespaces
                    .get_mut(table.namespace.as_str())
                    .unwrap()
                    .tables
                    .remove(&table.name);
            }
            Edit::ReplaceHttpHandler(handler) => {
                self.namespaces
                    .get_mut(handler.namespace.as_str())
                    .unwrap()
                    .http_handlers
                    .insert(handler.name.clone(), handler.clone());
            }
            Edit::DropHttpHandler(handler) => {
                self.namespaces
                    .get_mut(handler.namespace.as_str())
                    .unwrap()
                    .http_handlers
                    .remove(&handler.name);
            }
            Edit::ReplaceAuthenticationPolicy(policy) => {
                self.namespaces
                    .get_mut(policy.namespace.as_str())
                    .unwrap()
                    .authentication_policies
                    .insert(policy.name.clone(), policy.clone());
            }
            Edit::DropAuthenticationPolicy(policy) => {
                self.namespaces
                    .get_mut(policy.namespace.as_str())
                    .unwrap()
                    .authentication_policies
                    .remove(&policy.name);
            }
            Edit::ReplaceAuthorizationPolicy(policy) => {
                self.namespaces
                    .get_mut(policy.namespace.as_str())
                    .unwrap()
                    .authorization_policies
                    .insert(policy.name.clone(), policy.clone());
            }
            Edit::DropAuthorizationPolicy(policy) => {
                self.namespaces
                    .get_mut(policy.namespace.as_str())
                    .unwrap()
                    .authorization_policies
                    .remove(&policy.name);
            }
            Edit::Ddl(_) => todo!(),
        }

        Ok(())
    }
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
