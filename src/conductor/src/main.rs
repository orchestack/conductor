use std::path::PathBuf;

use anyhow::Result;
use arrow_cast::pretty;
use clap::{Parser, Subcommand, ValueHint};
use rustyline::{self, error::ReadlineError};
use score::Score;
use sql::SqlSession;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Compile(Compile),
    Diff(Diff),
    Apply(Apply),
    Sql(Sql),
}

/// Compile a score definition into a catalog representation.
#[derive(Parser, Debug)]
struct Compile {
    #[clap(name = "PATH", value_hint = ValueHint::FilePath)]
    score_path: PathBuf,
}

/// Show the difference between two score definitions.
#[derive(Parser, Debug)]
struct Diff {
    #[clap(name = "PATH_A", value_hint = ValueHint::FilePath)]
    a: PathBuf,

    #[clap(name = "PATH_B", value_hint = ValueHint::FilePath)]
    b: PathBuf,
}

/// Apply a score definition to an ensemble.
#[derive(Parser, Debug)]
struct Apply {
    #[clap(name = "PATH", value_hint = ValueHint::FilePath)]
    score_path: PathBuf,

    #[clap(long, value_enum, default_value_t = Ensemble::EnsembleX)]
    ensemble: Ensemble,

    /// Commit the changes to the ensemble. By default, the changes are tried
    /// in a dry-run mode.
    #[clap(long)]
    commit: bool,

    /// Path to the ensemble-x data.
    #[clap(long, value_hint = ValueHint::FilePath)]
    x_path: Option<PathBuf>,
}

/// Start a SQL session.
#[derive(Parser, Debug)]
struct Sql {
    #[clap(long, value_enum, default_value_t = Ensemble::EnsembleX)]
    ensemble: Ensemble,

    /// Path to the ensemble-x data.
    #[clap(long, value_hint = ValueHint::FilePath)]
    x_path: Option<PathBuf>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Ensemble {
    #[clap(name = "ensemble-x")]
    EnsembleX,
}

#[tokio::main]
async fn main() -> Result<()> {
    let global_args = Args::parse();
    match global_args.command {
        Command::Compile(args) => {
            let score = Score::new(args.score_path);
            let catalog = score.catalog()?;

            println!("Catalog: {:#?}", catalog);
        }
        Command::Diff(args) => {
            let a_score = Score::new(args.a);
            let a_catalog = a_score.catalog()?;

            let b_score = Score::new(args.b);
            let b_catalog = b_score.catalog()?;

            let diff = catalog::diff::Diff {};
            let statements = diff.diff(&a_catalog, &b_catalog)?;

            if statements.is_empty() {
                println!("No changes.");
            } else {
                for st in statements.into_iter() {
                    println!("{};", st);
                }
            }
        }
        Command::Apply(args) => match args.ensemble {
            Ensemble::EnsembleX => {
                let workspace = args.score_path;
                let data_path = args.x_path;
                let commit = args.commit;
                apply_ensemble_x(workspace, data_path, commit).await?;
            }
        },
        Command::Sql(args) => match args.ensemble {
            Ensemble::EnsembleX => {
                let data_path = args.x_path;
                sql_ensemble_x(data_path).await?;
            }
        },
    }

    Ok(())
}

async fn apply_ensemble_x(
    score_path: PathBuf,
    data_path: Option<PathBuf>,
    commit: bool,
) -> Result<()> {
    use ensemble_x::EnsembleX;

    let score = Score::new(score_path);
    let catalog = score.catalog()?;

    let mut ensemble = EnsembleX::with_path(data_path.expect("data_path must be provided")).await?;
    let from_catalog = ensemble.catalog()?;
    let diff = catalog::diff::Diff {};
    let edits = diff.diff(&from_catalog, &catalog)?;

    if edits.is_empty() {
        println!("No changes.");
        return Ok(());
    }

    for edit in edits.into_iter() {
        println!("{};", edit);

        if commit {
            ensemble.apply(&edit).await?;
        }
    }

    Ok(())
}

async fn sql_ensemble_x(data_path: Option<PathBuf>) -> Result<()> {
    use ensemble_x::EnsembleX;

    let ensemble = EnsembleX::with_path(data_path.expect("data_path must be provided")).await?;
    let mut session = SqlSession::new(ensemble).await?;

    let mut rl = rustyline::DefaultEditor::new()?;
    let rl_history_path = dirs::config_dir().unwrap().join(".conductor-sql-history");
    if !rl_history_path.exists() {
        std::fs::File::create(&rl_history_path)?;
    }

    if let Err(err) = rl.load_history(&rl_history_path) {
        println!("Failed to load previous history: {}", err);
    }

    let mut requested_interrupt = false;
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                requested_interrupt = false;
                if line == "exit" {
                    break;
                } else if line.trim() == "" {
                    continue;
                }

                rl.add_history_entry(&line)?;
                rl.append_history(&rl_history_path).ok();
                match session.execute(&line).await {
                    Ok(result) => pretty::print_batches(&result)?,
                    Err(err) => {
                        println!("Error: {}", err);
                        continue;
                    }
                }
            }
            Err(ReadlineError::Eof) => break,
            Err(ReadlineError::Interrupted) => {
                if requested_interrupt {
                    break;
                }
                requested_interrupt = true;
                println!("Interrupted (press Ctrl-C again to quit)");
            }
            Err(err) => println!("Err: {}", err),
        }
    }

    Ok(())
}
