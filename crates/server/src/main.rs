use tracing::*;
use tracing_subscriber::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if let Err(error) = tracing_subscriber::fmt().with_env_filter(filter).try_init() {
        warn!(error = %error, "failed to initialize tracing subscriber");
    }

    tungsten_server::run(None).await
}
