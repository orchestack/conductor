use std::collections::HashSet;

use catalog::{Catalog, Column, HttpHandler, Namespace, Table};

use crate::parser::Statement;
use crate::{Result, ScoreError, ScorePkg};

pub struct ScoreCompiler {}

impl ScoreCompiler {
    pub(crate) fn compile(&self, pkg: ScorePkg) -> Result<Catalog> {
        let mut catalog = Catalog::default();

        if !pkg.files.is_empty() {
            let ns = self.compile_pkg(pkg)?;
            catalog.namespaces.insert(ns.name.clone(), ns);
        }

        Ok(catalog)
    }

    fn compile_pkg(&self, pkg: ScorePkg) -> Result<Namespace> {
        // Parse namespace declaration from any file. Then we will just check
        // that all other files have the same namespace.
        let namespace_name = match pkg.files[0].statements.front().unwrap() {
            Statement::NamespaceDecl(name) => name,
            _ => return Err(ScoreError::Error("expected namespace declaration".into())),
        };

        let mut ns = Namespace {
            name: namespace_name.clone(),
            ..Default::default()
        };

        let mut table_names = HashSet::new();
        let mut table_uuids = HashSet::new();

        for file in &pkg.files {
            let mut stmt_iter = file.statements.iter();

            match stmt_iter.next() {
                Some(Statement::NamespaceDecl(name)) => {
                    if name != namespace_name {
                        return Err(ScoreError::Error(format!(
                            "conflicting namespace declaration {} and {} in the same package",
                            name, namespace_name
                        )));
                    }
                }
                _ => return Err(ScoreError::Error("expected namespace declaration".into())),
            };

            for stmt in stmt_iter {
                match stmt {
                    Statement::TableDecl(table_decl) => {
                        let mut table = Table {
                            namespace: ns.name.clone(),
                            uuid: Into::into(table_decl.uuid),
                            name: table_decl.name.clone(),
                            columns: Default::default(),
                        };

                        let mut column_names = HashSet::new();
                        let mut column_uids = HashSet::new();

                        for col in &table_decl.columns {
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
                                name: col.inner.name.value.clone(),
                                data_type: col.inner.data_type.clone(),
                            });
                        }

                        if table_names.contains(&table_decl.name)
                            || table_uuids.contains(&table_decl.uuid)
                        {
                            return Err(ScoreError::CompileError {
                                error: format!(
                                    "conflicting table declaration: {}",
                                    table_decl.name
                                ),
                                path: file.path.clone(),
                            });
                        }

                        table_names.insert(table_decl.name.clone());
                        table_uuids.insert(table_decl.uuid);

                        ns.tables.insert(table_decl.name.clone(), table);
                    }
                    Statement::HttpHandlerDecl(handler_decl) => {
                        if ns.http_handlers.contains_key(&handler_decl.name) {
                            return Err(ScoreError::CompileError {
                                error: format!(
                                    "conflicting http handler declaration: {}",
                                    handler_decl.name
                                ),
                                path: file.path.clone(),
                            });
                        }

                        ns.http_handlers.insert(
                            handler_decl.name.clone(),
                            HttpHandler {
                                namespace: ns.name.clone(),
                                name: handler_decl.name.clone(),
                                body: match &handler_decl.body {
                                    sql::parser::Statement::Statement(st) => st.to_string(),
                                },
                            },
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(ns)
    }
}
