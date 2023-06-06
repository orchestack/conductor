use std::collections::VecDeque;

use catalog::{Catalog, Column, Namespace, Table};
use thiserror::Error;

use crate::parser::Statement;

#[derive(Error, Debug)]
pub enum CompileError {
    #[error("compile error {:?}", .0)]
    CompileError(String),
}

pub struct ScoreCompiler {}

impl ScoreCompiler {
    pub fn compile(&self, mut stmts: VecDeque<Statement>) -> Result<Catalog, CompileError> {
        let mut ns = match stmts.pop_front().unwrap() {
            Statement::NamespaceDecl(name) => Namespace {
                name: name.value,
                tables: Default::default(),
            },
            _ => {
                return Err(CompileError::CompileError(
                    "expected namespace name first".into(),
                ))
            }
        };

        while let Some(stmt) = stmts.pop_front() {
            match stmt {
                Statement::TableDecl(ct) => {
                    let mut table = Table {
                        uuid: Into::into(ct.uuid),
                        name: ct.name.to_string(),
                        columns: Default::default(),
                    };
                    for col in ct.columns {
                        table.columns.push(Column {
                            uid: col.uid,
                            name: col.inner.name.value,
                            data_type: col.inner.data_type,
                        });
                    }

                    ns.tables.insert(ct.name.to_string(), table);
                }
                _ => return Err(CompileError::CompileError("tbd".into())),
            }
        }

        Ok(Catalog { root: ns })
    }
}
