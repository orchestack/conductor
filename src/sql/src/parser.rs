use std::collections::VecDeque;

use crate::Error;
use sqlparser::{
    ast::Statement as SqlStatement,
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::{TokenWithLocation, Tokenizer},
};

type Result<T> = std::result::Result<T, Error>;

pub enum Statement {
    Statement(SqlStatement),
}

pub(crate) struct SqlParser<'a> {
    inner: Parser<'a>,
}

impl<'a> SqlParser<'a> {
    pub(crate) fn new(sql: &'a str) -> Result<Self> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(SqlParser {
            inner: Parser::new(dialect).with_tokens(tokens),
        })
    }

    pub(crate) fn parse_sql(&mut self) -> Result<VecDeque<Statement>> {
        Ok(self
            .inner
            .parse_statements()?
            .into_iter()
            .map(Statement::Statement)
            .collect())
    }

    fn _expected<T>(&self, expected: &str, found: TokenWithLocation) -> Result<T> {
        Err(Error::Error(format!(
            "Expected {expected}, found: {found} at Line: {}, Column {}",
            found.location.line, found.location.column,
        )))
    }
}
