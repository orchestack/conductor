use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueHint};
use score::Score;

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

            for st in statements.into_iter() {
                println!("{};", st);
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

    let ensemble = EnsembleX::with_path(data_path.expect("data_path must be provided"));
    let from_catalog = ensemble.catalog()?;
    let diff = catalog::diff::Diff {};
    let edits = diff.diff(&from_catalog, &catalog)?;

    for edit in edits.into_iter() {
        println!("{};", edit);

        if commit {
            ensemble.apply(&edit).await?;
        }
    }

    Ok(())
}
