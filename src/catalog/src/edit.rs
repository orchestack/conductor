use std::fmt::Display;

use crate::Table;
use sqlparser::ast::Statement;

#[derive(Debug)]
pub enum Edit {
    CreateNamespace { name: String },
    CreateTable(Table),
    Ddl(Statement),
}

impl Display for Edit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Edit::CreateNamespace { name } => write!(f, "CREATE NAMESPACE {}", name),
            Edit::CreateTable(table) => {
                write!(f, "CREATE {:?}", table)
            }
            Edit::Ddl(stmt) => write!(f, "{}", stmt),
        }
    }
}

impl From<Statement> for Edit {
    fn from(value: Statement) -> Self {
        Self::Ddl(value)
    }
}
