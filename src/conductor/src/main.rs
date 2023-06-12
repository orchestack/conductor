use std::{path::PathBuf, sync::Arc};

use anyhow::{anyhow, bail, Result};
use arrow_cast::pretty;
use clap::{Parser, Subcommand, ValueHint};
use ensemble_x::storage::ObjectStore;
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, prefix::PrefixStore};
use rustyline::{self, error::ReadlineError};
use score::Score;
use sql::SqlSession;
use url::Url;

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
    /// Can be s3://, gs:// or just a local path.
    #[clap(long)]
    x_path: Option<String>,
}

/// Start a SQL session.
#[derive(Parser, Debug)]
struct Sql {
    #[clap(long, value_enum, default_value_t = Ensemble::EnsembleX)]
    ensemble: Ensemble,

    /// Path to the ensemble-x data.
    /// Can be s3://, gs:// or just a local path.
    #[clap(long)]
    x_path: Option<String>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Ensemble {
    #[clap(name = "ensemble-x")]
    EnsembleX,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

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
    data_path: Option<String>,
    commit: bool,
) -> Result<()> {
    use ensemble_x::EnsembleX;

    let score = Score::new(score_path);
    let catalog = score.catalog()?;

    let store = configure_ensemble_x_storage(data_path.unwrap())?;
    let mut ensemble = EnsembleX::new(store).await?;
    let from_catalog = ensemble.catalog()?;
    let diff = catalog::diff::Diff {};
    let edits = diff.diff(&from_catalog, &catalog)?;

    if edits.is_empty() {
        println!("No changes.");
        return Ok(());
    }

    for edit in edits.into_iter() {
        println!("{};", edit);
        ensemble.apply(&edit).await?;
    }

    if commit {
        ensemble.commit().await?;
    }

    Ok(())
}

async fn sql_ensemble_x(data_path: Option<String>) -> Result<()> {
    use ensemble_x::EnsembleX;

    let store = configure_ensemble_x_storage(data_path.unwrap())?;
    let ensemble = EnsembleX::new(store).await?;
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

fn parse_data_path(data_path: String) -> Result<Url> {
    if let Ok(url) = Url::parse(data_path.as_str()) {
        return Ok(url);
    } else {
        let path = PathBuf::from(data_path.as_str());
        if path.exists() {
            let abs_path = path.canonicalize()?;
            let url = Url::from_directory_path(abs_path).unwrap();
            return Ok(url);
        }
    }

    Err(anyhow!(
        "Invalid data path: {}. If it is a local path, make sure it exists.",
        data_path
    ))
}

fn configure_object_storage(uri: &Url) -> Result<Box<object_store::DynObjectStore>> {
    match uri.scheme() {
        "file" => {
            let path = uri.to_file_path().unwrap();
            let storage = object_store::local::LocalFileSystem::new_with_prefix(path)?;

            Ok(Box::new(storage))
        }
        "gs" => {
            let gcs = GoogleCloudStorageBuilder::from_env()
                .with_url(uri.as_str())
                .build()?;

            Ok(Box::new(PrefixStore::new(gcs, uri.path().to_string())))
        }
        "s3" => {
            let s3 = AmazonS3Builder::from_env().with_url(uri.as_str()).build()?;

            Ok(Box::new(PrefixStore::new(s3, uri.path().to_string())))
        }
        scheme => {
            bail!("Unsupported scheme: {}", scheme)
        }
    }
}

fn configure_ensemble_x_storage(data_path: String) -> Result<ensemble_x::storage::ObjectStore> {
    let location = parse_data_path(data_path)?;
    let storage = configure_object_storage(&location)?;

    Ok(ObjectStore::new(
        Arc::new(storage),
        location,
        // HACK: unsafe_rename is required for the S3 backend to work
        /*unsafe_rename=*/
        true,
    ))
}
