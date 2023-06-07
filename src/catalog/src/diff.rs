use std::collections::HashSet;

use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, Ident, ObjectName, ObjectType, Statement,
};
use thiserror::Error;

use crate::{edit::Edit, Catalog, Namespace, Table};

#[derive(Error, Debug)]
pub enum DiffError {
    #[error("diff error {:?}", .0)]
    DiffError(String),
}

pub struct Diff {}

impl Diff {
    pub fn diff(&self, a: &Catalog, b: &Catalog) -> Result<Vec<Edit>, DiffError> {
        self.diff_namespace(&a.root, &b.root)
    }

    fn diff_namespace(&self, a: &Namespace, b: &Namespace) -> Result<Vec<Edit>, DiffError> {
        assert_eq!(a.name, b.name, "namespace names must match");

        let a_table_ids = a.tables.values().map(|v| v.uuid).collect::<HashSet<_>>();
        let b_table_ids = b.tables.values().map(|v| v.uuid).collect::<HashSet<_>>();

        let mut edits = Vec::<Edit>::new();

        // Tables that exist in A but not B, we need to drop them.
        let drop_tables = a_table_ids.difference(&b_table_ids).collect::<Vec<_>>();
        for table_id in drop_tables {
            let table = &a.get_table_by_uuid(*table_id).unwrap();

            edits.push(
                Statement::Drop {
                    object_type: ObjectType::Table,
                    if_exists: false,
                    names: vec![ObjectName(vec![identifier(&table.name)])],
                    cascade: false,
                    restrict: false,
                    purge: false,
                }
                .into(),
            );
        }

        // Tables that exist in B but not A, we need to create them.
        let create_tables = b_table_ids.difference(&a_table_ids).collect::<Vec<_>>();
        for table_id in create_tables {
            let table = b.get_table_by_uuid(*table_id).unwrap();
            edits.push(Edit::CreateTable((*table).clone()));
        }

        // Tables that exist in both A and B, we need to diff them.
        let diff_tables = a_table_ids.intersection(&b_table_ids).collect::<Vec<_>>();
        for table_id in diff_tables {
            let a_table = a.get_table_by_uuid(*table_id).unwrap();
            let b_table = b.get_table_by_uuid(*table_id).unwrap();

            edits.extend(self.diff_table(a_table, b_table)?);
        }

        Ok(edits)
    }

    fn diff_table(&self, a: &Table, b: &Table) -> Result<Vec<Edit>, DiffError> {
        assert_eq!(a.uuid, b.uuid, "table uuids must match");

        let mut stmts = vec![];
        let mut alter_ops = Vec::new();
        let mut table_name = ObjectName(vec![identifier(&a.name)]);

        if a.name != b.name {
            table_name = ObjectName(vec![identifier(&b.name)]);
            stmts.push(
                Statement::AlterTable {
                    name: ObjectName(vec![identifier(&a.name)]),
                    operation: AlterTableOperation::RenameTable {
                        table_name: table_name.clone(),
                    },
                }
                .into(),
            );
        }

        let a_column_ids = a.columns.iter().map(|v| v.uid).collect::<HashSet<_>>();
        let b_column_ids = b.columns.iter().map(|v| v.uid).collect::<HashSet<_>>();

        // Columns that exist in A but not B, we need to drop them.
        let drop_columns = a_column_ids.difference(&b_column_ids).collect::<Vec<_>>();
        for column_id in drop_columns {
            let column = &a.get_column_by_uid(*column_id).unwrap();

            alter_ops.push(AlterTableOperation::DropColumn {
                column_name: identifier(&column.name),
                if_exists: false,
                cascade: false,
            });
        }

        // Columns that exist in B but not A, we need to create them.
        let create_columns = b_column_ids.difference(&a_column_ids).collect::<Vec<_>>();
        for column_id in create_columns {
            let column = b.get_column_by_uid(*column_id).unwrap();

            alter_ops.push(AlterTableOperation::AddColumn {
                column_def: ColumnDef {
                    name: identifier(&column.name),
                    data_type: column.data_type.clone(),
                    collation: None,
                    options: vec![],
                },
                column_keyword: true,
                if_not_exists: false,
            });
        }

        // Columns that exist in both A and B, we need to diff them.
        let diff_columns = a_column_ids.intersection(&b_column_ids).collect::<Vec<_>>();
        for column_id in diff_columns {
            let a_column = a.get_column_by_uid(*column_id).unwrap();
            let b_column = b.get_column_by_uid(*column_id).unwrap();

            if a_column.name != b_column.name {
                alter_ops.push(AlterTableOperation::RenameColumn {
                    old_column_name: identifier(&a_column.name),
                    new_column_name: identifier(&b_column.name),
                });
            }

            if a_column.data_type != b_column.data_type {
                alter_ops.push(AlterTableOperation::AlterColumn {
                    column_name: identifier(&b_column.name),
                    op: AlterColumnOperation::SetDataType {
                        data_type: b_column.data_type.clone(),
                        using: None,
                    },
                });
            }
        }

        for alter_op in alter_ops.drain(..) {
            stmts.push(
                Statement::AlterTable {
                    name: table_name.clone(),
                    operation: alter_op,
                }
                .into(),
            );
        }

        Ok(stmts)
    }
}

fn identifier<S: Into<String>>(s: S) -> Ident {
    Ident::with_quote('"', s)
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_diff() {}
}
