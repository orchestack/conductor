use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueHint};

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
    workspace: PathBuf,
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
    workspace: PathBuf,

    #[clap(long, value_enum, default_value_t = Ensemble::DeltaLake)]
    ensemble: Ensemble,

    /// Commit the changes to the ensemble. By default, the changes are tried
    /// in a dry-run mode.
    #[clap(long)]
    commit: bool,

    /// Path to the Delta Lake ensemble.
    #[clap(long, value_hint = ValueHint::FilePath)]
    deltalake_path: Option<PathBuf>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Ensemble {
    #[clap(name = "deltalake")]
    DeltaLake,
}

#[tokio::main]
async fn main() -> Result<()> {
    let global_args = Args::parse();
    match global_args.command {
        Command::Compile(args) => {
            let workspace = args.workspace;
            let contents = std::fs::read_to_string(&workspace)?;
            let mut parser = score::parser::ScoreParser::new(&contents)?;
            let statements = parser
                .parse()
                .with_context(|| format!("while parsing {}", workspace.display()))?;
            let compiler = score::compiler::ScoreCompiler {};
            let catalog = compiler.compile(statements)?;

            println!("Catalog: {:#?}", catalog);
        }
        Command::Diff(args) => {
            let a = args.a;
            let b = args.b;
            let a_contents = std::fs::read_to_string(&a)?;
            let b_contents = std::fs::read_to_string(&b)?;
            let mut a_parser = score::parser::ScoreParser::new(&a_contents)?;
            let mut b_parser = score::parser::ScoreParser::new(&b_contents)?;
            let a_statements = a_parser
                .parse()
                .with_context(|| format!("while parsing {}", a.display()))?;
            let b_statements = b_parser
                .parse()
                .with_context(|| format!("while parsing {}", b.display()))?;
            let compiler = score::compiler::ScoreCompiler {};
            let a_catalog = compiler.compile(a_statements)?;
            let b_catalog = compiler.compile(b_statements)?;
            let diff = catalog::diff::Diff {};
            let statements = diff.diff(&a_catalog, &b_catalog)?;

            for st in statements.into_iter() {
                println!("{};", st);
            }
        }
        Command::Apply(args) => match args.ensemble {
            Ensemble::DeltaLake => {
                let workspace = args.workspace;
                let deltalake_path = args.deltalake_path;
                let commit = args.commit;
                apply_deltalake(workspace, deltalake_path, commit).await?;
            }
        },
    }

    Ok(())
}

async fn apply_deltalake(
    workspace: PathBuf,
    deltalake_path: Option<PathBuf>,
    commit: bool,
) -> Result<()> {
    use ensemble_x::EnsembleX;

    let contents = std::fs::read_to_string(&workspace)?;
    let mut parser = score::parser::ScoreParser::new(&contents)?;
    let statements = parser
        .parse()
        .with_context(|| format!("while parsing {}", workspace.display()))?;
    let compiler = score::compiler::ScoreCompiler {};
    let catalog = compiler.compile(statements)?;

    let ensemble =
        EnsembleX::with_deltalake_path(deltalake_path.expect("deltalake_path must be provided"));
    let from_catalog = ensemble.catalog()?;
    let diff = catalog::diff::Diff {};
    let statements = diff.diff(&from_catalog, &catalog)?;

    for st in statements.into_iter() {
        println!("{};", st);

        if commit {
            ensemble.apply(&st).await?;
        }
    }

    Ok(())
}
