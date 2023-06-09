use std::collections::VecDeque;

use sql::parser::SqlParser;
use sqlparser::{
    ast::{DollarQuotedString, Ident, TableConstraint, Value},
    dialect::GenericDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{self, Token, TokenWithLocation, Tokenizer},
};

use crate::{Result, ScoreError};

#[derive(Debug)]
pub enum Statement {
    NamespaceDecl(String),
    TableDecl(TableDecl),
    HttpHandlerDecl(HttpHandlerDecl),
    AuthenticationPolicyDecl(AuthenticationPolicyDecl),
    AuthorizationPolicyDecl(AuthorizationPolicyDecl),
}

#[derive(Debug)]
pub struct TableDecl {
    pub name: String,
    pub uuid: uuid::Uuid,
    pub columns: Vec<ColumnDef>,
    // pub constraints: Vec<TableConstraint>,
}

#[derive(Debug)]
pub struct ColumnDef {
    pub uid: u32,
    pub inner: sqlparser::ast::ColumnDef,
}

#[derive(Debug)]
pub struct HttpHandlerDecl {
    pub name: String,
    pub policy: String,
    pub body: sql::parser::Statement,
}

#[derive(Debug)]
pub struct AuthenticationPolicyDecl {
    pub name: String,
    pub typ: String,
}

#[derive(Debug)]
pub struct AuthorizationPolicyDecl {
    pub name: String,
    pub permissive_expr: sqlparser::ast::Expr,
}

pub struct ScoreParser<'a> {
    parser: Parser<'a>,
}

impl<'a> ScoreParser<'a> {
    pub fn new(sql: &str) -> Result<Self> {
        let dialect = &GenericDialect {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer
            .tokenize_with_location()
            .map_err(|e| ScoreError::ParserError(e.into()))?;

        Ok(Self {
            parser: Parser::new(dialect).with_tokens_with_locations(tokens),
        })
    }

    pub fn parse(&mut self) -> Result<VecDeque<Statement>> {
        let mut out = VecDeque::new();
        let namespace = self.parse_namespace_decl()?;
        out.push_back(Statement::NamespaceDecl(namespace.value));

        loop {
            if self.parser.peek_token().token == Token::EOF {
                break;
            }
            self.parser.expect_token(&Token::SemiColon)?;
            if self.parser.peek_token().token == Token::EOF {
                break;
            }
            let stmt = self.parse_statement()?;
            out.push_back(stmt);
        }
        self.parser.expect_token(&Token::EOF)?;

        Ok(out)
    }

    fn parse_namespace_decl(&mut self) -> Result<Ident> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.value.to_lowercase().as_str() {
                "namespace" => {
                    self.parser.next_token();
                    let n = self.parser.parse_identifier()?;
                    Ok(n)
                }
                _ => self.expected(
                    "NAMESPACE <single quoted namespace string>",
                    self.peek_token(),
                ),
            },
            _ => self.expected(
                "NAMESPACE <single quoted namespace string>",
                self.peek_token(),
            ),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        let entity_types = vec![
            "TABLE",
            "HTTP_HANDLER",
            "AUTHENTICATION_POLICY",
            "AUTHORIZATION_POLICY",
        ];

        if let Token::Word(w) = self.peek_token().token {
            match w.value.as_ref() {
                "TABLE" => {
                    self.parser.next_token();
                    return self.parse_table_decl();
                }
                "HTTP_HANDLER" => {
                    self.parser.next_token();
                    return self.parse_http_handler_decl();
                }
                "AUTHENTICATION_POLICY" => {
                    self.parser.next_token();
                    return self.parse_authentication_policy_decl();
                }
                "AUTHORIZATION_POLICY" => {
                    self.parser.next_token();
                    return self.parse_authorization_policy_decl();
                }
                _ => {}
            }
        }

        self.expected(&format!("one of {:?}", entity_types), self.peek_token())
    }

    fn parse_table_decl(&mut self) -> Result<Statement> {
        let name = self.parser.parse_identifier()?;
        let uuid = self.parse_table_uuid()?;
        let (columns, _constraints) = self.parse_columns()?;

        Ok(Statement::TableDecl(TableDecl {
            name: name.value,
            uuid,
            columns,
            // constraints,
        }))
    }

    fn parse_table_uuid(&mut self) -> Result<uuid::Uuid> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.value.to_lowercase().as_str() {
                "uuid" => {
                    self.parser.next_token();
                    let twl = self.peek_token();
                    match self.parser.parse_value()? {
                        Value::SingleQuotedString(s) => match uuid::Uuid::parse_str(&s) {
                            Ok(uuid) => Ok(uuid),
                            Err(_) => self.expected("valid uuid value", twl),
                        },
                        _ => self.expected("single quoted uuid string", twl),
                    }
                }
                _ => self.expected("UUID <single quoted uuid string>", self.peek_token()),
            },
            _ => self.expected("UUID <single quoted uuid string>", self.peek_token()),
        }
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>)> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.peek_token().token {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected("column name or constraint definition", self.peek_token());
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after column definition", self.peek_token());
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        // let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
        //     Some(self.parser.parse_object_name()?)
        // } else {
        //     None
        // };
        // let mut options = vec![];
        // loop {
        //     if self.parser.parse_keyword(Keyword::CONSTRAINT) {
        //         let name = Some(self.parser.parse_identifier()?);
        //         if let Some(option) = self.parser.parse_optional_column_option()? {
        //             options.push(ColumnOptionDef { name, option });
        //         } else {
        //             return self.expected(
        //                 "constraint details after CONSTRAINT <name>",
        //                 self.peek_token(),
        //             );
        //         }
        //     } else if let Some(option) = self.parser.parse_optional_column_option()? {
        //         options.push(ColumnOptionDef { name: None, option });
        //     } else {
        //         break;
        //     };
        // }

        let uid = self.parse_column_def_uid()?;

        Ok(ColumnDef {
            uid,
            inner: sqlparser::ast::ColumnDef {
                name,
                data_type,
                collation: None,
                options: vec![],
            },
        })
    }

    fn parse_column_def_uid(&mut self) -> Result<u32> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.value.to_lowercase().as_str() {
                "uid" => {
                    self.parser.next_token();
                    match self.parser.parse_value()? {
                        sqlparser::ast::Value::Number(num, _) => num
                            .parse::<u32>()
                            .map_err(|e| ScoreError::Error(e.to_string())),
                        _ => self.expected("literal number", self.peek_token()),
                    }
                }
                _ => self.expected("UID <literal number>", self.peek_token()),
            },
            _ => self.expected("UID <literal number>", self.peek_token()),
        }
    }

    fn parse_http_handler_decl(&mut self) -> Result<Statement> {
        let name = self.parser.parse_identifier()?;

        self.parser.expect_token(&Token::Word(tokenizer::Word {
            value: "POLICY".to_string(),
            quote_style: None,
            keyword: Keyword::NoKeyword,
        }))?;
        let policy = self.parser.parse_identifier()?;

        self.parser.expect_keyword(Keyword::AS)?;
        let body = match self.peek_token().token {
            Token::DollarQuotedString(DollarQuotedString { value, .. }) => {
                self.parser.next_token();

                let mut sql_stmt_parser = SqlParser::new(&value)?;
                sql_stmt_parser.parse_sql()?.into_iter().next().unwrap()
            }
            _ => return self.expected("dollar quoted string", self.peek_token()),
        };

        let handler_decl = HttpHandlerDecl {
            name: name.value,
            policy: policy.value,
            body,
        };
        Ok(Statement::HttpHandlerDecl(handler_decl))
    }

    fn peek_token(&self) -> TokenWithLocation {
        self.parser.peek_token()
    }

    fn parse_authentication_policy_decl(&mut self) -> Result<Statement> {
        let name = self.parser.parse_identifier()?;

        self.parser.expect_keyword(Keyword::TYPE)?;
        self.parser.expect_token(&Token::Eq)?;
        self.parser.expect_token(&Token::Word(tokenizer::Word {
            value: "anonymous".to_string(),
            quote_style: None,
            keyword: Keyword::NoKeyword,
        }))?;

        Ok(Statement::AuthenticationPolicyDecl(
            AuthenticationPolicyDecl {
                name: name.value,
                typ: "anonymous".to_string(),
            },
        ))
    }

    fn parse_authorization_policy_decl(&mut self) -> Result<Statement> {
        let name = self.parser.parse_identifier()?;

        self.parser.expect_token(&Token::Word(tokenizer::Word {
            value: "permissive_expr".to_string(),
            quote_style: None,
            keyword: Keyword::NoKeyword,
        }))?;
        self.parser.expect_token(&Token::Eq)?;
        let permissive_expr = self.parser.parse_expr()?;

        Ok(Statement::AuthorizationPolicyDecl(
            AuthorizationPolicyDecl {
                name: name.value,
                permissive_expr,
            },
        ))
    }

    fn expected<T>(&self, expected: &str, found: TokenWithLocation) -> Result<T> {
        Err(ScoreError::Error(format!(
            "Expected {expected}, found: {found} at Line: {}, Column {}",
            found.location.line, found.location.column,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let sql = "
            NAMESPACE northwind;

            TABLE foo
            UUID 'E709EBE9-8B6C-4BD6-80DA-5629D1B64039'
            (
                id INTEGER UID 1,
                name TEXT UID 2,
                age INTEGER UID 3
            );

            TABLE bar
            UUID '9B972E4A-D412-48CD-9290-7BD2A192966B'
            (
                id INTEGER UID 1,
                name TEXT UID 2,
                age INTEGER UID 3
            );
        ";
        let stmts = ScoreParser::new(sql).unwrap().parse().unwrap();
        println!("{:?}", stmts);
    }
}
