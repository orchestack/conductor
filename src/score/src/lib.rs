use std::{collections::VecDeque, fs, io, path::PathBuf};

pub mod compiler;
pub mod parser;

use catalog::Catalog;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ScoreError {
    #[error("parser error: {0}")]
    ParserError(#[from] sqlparser::parser::ParserError),
    #[error("io error: {0}")]
    IoError(#[from] io::Error),
    #[error("score error: {0}")]
    Error(String),
}

pub type Result<T> = std::result::Result<T, ScoreError>;

pub struct Score {
    path: PathBuf,
}

impl Score {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Parse the score file and return a catalog.
    pub fn catalog(&self) -> Result<Catalog> {
        let score_files = fs::read_dir(&self.path)?
            .map(|entry| -> Result<_> {
                let entry = entry?;
                if !entry.path().is_file() {
                    return Err(ScoreError::Error(format!(
                        "sub-directories are not supported yet: {}",
                        entry.path().display()
                    )));
                }

                Ok(entry.path())
            })
            .collect::<Result<Vec<_>>>()?;

        // Collect all statements from all score files.
        let mut statements = VecDeque::new();
        for score_file in score_files {
            let content = fs::read_to_string(score_file)?;
            let mut sp = parser::ScoreParser::new(&content)?;
            let score_file = sp.parse()?;
            statements.extend(score_file);
        }

        // Compile the statements into a catalog.
        let compiler = compiler::ScoreCompiler {};
        let catalog = compiler.compile(statements)?;

        Ok(catalog)
    }
}
