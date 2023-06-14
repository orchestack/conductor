use std::fmt::Display;

use sqlparser::ast::Statement;

use crate::{HttpHandler, Table};

#[derive(Debug)]
pub enum Edit {
    CreateNamespace { name: String },
    CreateTable(Table),
    DropTable(Table),
    ReplaceHttpHandler(HttpHandler),
    DropHttpHandler(HttpHandler),
    Ddl(Statement),
}

impl Display for Edit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Edit::CreateNamespace { name } => write!(f, "CREATE NAMESPACE {}", name),
            Edit::CreateTable(table) => write!(f, "CREATE {:?}", table),
            Edit::DropTable(table) => write!(f, "DROP {:?}", table),
            handler @ Edit::ReplaceHttpHandler { .. } => write!(f, "REPLACE {:?}", handler),
            handler @ Edit::DropHttpHandler { .. } => write!(f, "DROP {:?}", handler),
            Edit::Ddl(stmt) => write!(f, "{}", stmt),
        }
    }
}

impl From<Statement> for Edit {
    fn from(value: Statement) -> Self {
        Self::Ddl(value)
    }
}
