use std::collections::{HashSet, VecDeque};

use catalog::{Catalog, Column, Namespace, Table};

use crate::parser::Statement;
use crate::{Result, ScoreError};

pub struct ScoreCompiler {}

impl ScoreCompiler {
    pub fn compile(&self, mut stmts: VecDeque<Statement>) -> Result<Catalog> {
        let mut table_names = HashSet::new();
        let mut table_uuids = HashSet::new();

        let mut ns = match stmts.pop_front().unwrap() {
            Statement::NamespaceDecl(name) => Namespace {
                name,
                tables: Default::default(),
            },
            _ => return Err(ScoreError::Error("expected namespace declaration".into())),
        };

        while let Some(stmt) = stmts.pop_front() {
            match stmt {
                Statement::TableDecl(ct) => {
                    let mut table = Table {
                        namespace: ns.name.clone(),
                        uuid: Into::into(ct.uuid),
                        name: ct.name.clone(),
                        columns: Default::default(),
                    };

                    let mut column_names = HashSet::new();
                    let mut column_uids = HashSet::new();

                    for col in ct.columns {
                        if column_names.contains(&col.inner.name.value)
                            || column_uids.contains(&col.uid)
                        {
                            return Err(ScoreError::Error(format!(
                                "conflicting column declaration {} {}",
                                col.inner.name.value, col.uid
                            )));
                        }

                        column_names.insert(col.inner.name.value.clone());
                        column_uids.insert(col.uid);

                        table.columns.push(Column {
                            uid: col.uid,
                            name: col.inner.name.value,
                            data_type: col.inner.data_type,
                        });
                    }

                    if table_names.contains(&ct.name) || table_uuids.contains(&ct.uuid) {
                        return Err(ScoreError::Error(format!(
                            "conflicting table declaration: {}",
                            ct.name
                        )));
                    }

                    table_names.insert(ct.name.clone());
                    table_uuids.insert(ct.uuid);

                    ns.tables.insert(ct.name, table);
                }
                _ => unreachable!(),
            }
        }

        Ok(Catalog { root: ns })
    }
}
