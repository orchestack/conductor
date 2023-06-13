use std::{collections::VecDeque, fs, io, path::PathBuf};

pub mod compiler;
pub mod parser;

use catalog::Catalog;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ScoreError {
    #[error("compile error: {error} in {path}")]
    CompileError { error: String, path: PathBuf },
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

        let mut pkg = ScorePkg {
            path: self.path.clone(),
            files: Vec::new(),
        };

        // Collect all statements from all score files.
        for score_file in score_files {
            let content = fs::read_to_string(&score_file)?;
            let mut sp = parser::ScoreParser::new(&content)?;
            let statements = sp.parse()?;

            pkg.files.push(ScoreFile {
                path: score_file.clone(),
                statements,
            });
        }

        // Compile the statements into a catalog.
        let compiler = compiler::ScoreCompiler {};
        let catalog = compiler.compile(pkg)?;

        Ok(catalog)
    }
}

struct ScorePkg {
    #[allow(dead_code)]
    path: PathBuf,
    files: Vec<ScoreFile>,
}

struct ScoreFile {
    path: PathBuf,
    statements: VecDeque<parser::Statement>,
}
