use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use axum_macros::debug_handler;
use catalog::auth::AuthEval;
use catalog::AuthenticationPolicyType;
use clap::Parser;

use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};

use ensemble_x::storage::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::prefix::PrefixStore;
use tracing::{info, trace};
use url::Url;

use crate::http_handler_input::HttpHandlerInput;

mod http_handler_input;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "127.0.0.1:3000")]
    addr: SocketAddr,

    /// Path to the ensemble-x data.
    /// Can be s3://, gs:// or just a local path.
    #[clap(long)]
    x_path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let state = Arc::new(AppState::new(args.x_path.unwrap())?);

    let app = Router::new()
        .route("/ns/:ns/handler/:handler", post(http_handler))
        .with_state(state);
    let addr = args.addr;

    info!(?addr, "server listening");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .with_context(|| format!("failed to bind to {}", addr))
}

#[debug_handler]
async fn http_handler(
    State(state): State<Arc<AppState>>,
    Path((ns_name, handler_name)): Path<(String, String)>,
    body: String,
) -> impl IntoResponse {
    // Let's load the catalog.
    let ensemble = state.ensemble_x().await.unwrap();
    let catalog = ensemble.catalog().unwrap();

    let anonymous_access_allowed = catalog
        .namespaces
        .get(&ns_name)
        .unwrap()
        .authentication_policies
        .values()
        .any(|p| matches!(p.typ, AuthenticationPolicyType::Anonymous()));
    if !anonymous_access_allowed {
        trace!("anonymous access not allowed");
        return (StatusCode::UNAUTHORIZED, ());
    }

    let handler = catalog
        .namespaces
        .get(&ns_name)
        .unwrap()
        .http_handlers
        .get(&handler_name)
        .unwrap();

    let policy = catalog
        .namespaces
        .get(&ns_name)
        .unwrap()
        .authorization_policies
        .get(&handler.policy)
        .unwrap();

    let auth_eval = AuthEval::default();
    if !auth_eval.eval_policy(policy) {
        return (StatusCode::UNAUTHORIZED, ());
    }

    info!(?handler, "http handler");

    // TODO: Load into session only objects that are needed by the http handler.
    let mut session = sql::SqlSession::new(ensemble).await.unwrap();

    let schema = MemorySchemaProvider::new();

    let input = HttpHandlerInput::new(body);
    schema
        .register_table("input".to_string(), Arc::new(input))
        .unwrap();

    session
        .state
        .catalog_list()
        .catalog("conductor")
        .unwrap()
        .register_schema("temporary", Arc::new(schema))
        .unwrap();

    session.execute(handler.body.as_str()).await.unwrap();

    (StatusCode::OK, ())
}

struct AppState {
    object_store: ObjectStore,
}

impl AppState {
    fn new(data_path: String) -> Result<Self> {
        let object_store = configure_ensemble_x_storage(data_path)?;

        Ok(Self { object_store })
    }

    async fn ensemble_x(&self) -> Result<ensemble_x::EnsembleX> {
        Ok(ensemble_x::EnsembleX::new(self.object_store.clone()).await?)
    }
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

    bail!(
        "Invalid data path: {}. If it is a local path, make sure it exists.",
        data_path
    )
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

fn configure_ensemble_x_storage(data_path: String) -> Result<ObjectStore> {
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
