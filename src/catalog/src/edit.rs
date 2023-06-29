use std::fmt::Display;

use crate::{AuthenticationPolicy, AuthorizationPolicy, HttpHandler, Table};

#[derive(Debug)]
pub enum Edit {
    CreateNamespace { name: String },

    CreateTable(Table),
    DropTable(Table),

    ReplaceHttpHandler(HttpHandler),
    DropHttpHandler(HttpHandler),

    ReplaceAuthenticationPolicy(AuthenticationPolicy),
    DropAuthenticationPolicy(AuthenticationPolicy),

    ReplaceAuthorizationPolicy(AuthorizationPolicy),
    DropAuthorizationPolicy(AuthorizationPolicy),
}

impl Display for Edit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Edit::CreateNamespace { name } => write!(f, "CREATE NAMESPACE {}", name),

            Edit::CreateTable(table) => write!(f, "CREATE {:?}", table),
            Edit::DropTable(table) => write!(f, "DROP {:?}", table),

            handler @ Edit::ReplaceHttpHandler { .. } => write!(f, "REPLACE {:?}", handler),
            handler @ Edit::DropHttpHandler { .. } => write!(f, "DROP {:?}", handler),

            policy @ Edit::ReplaceAuthenticationPolicy(_) => write!(f, "REPLACE {:?}", policy),
            policy @ Edit::DropAuthenticationPolicy(_) => write!(f, "DROP {:?}", policy),

            policy @ Edit::ReplaceAuthorizationPolicy(_) => write!(f, "REPLACE {:?}", policy),
            policy @ Edit::DropAuthorizationPolicy(_) => write!(f, "DROP {:?}", policy),
        }
    }
}
