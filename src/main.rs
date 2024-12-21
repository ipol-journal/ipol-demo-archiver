use std::time::Duration;

use anyhow::{Context, Result};
use bollard::Docker;
use clap::Parser;
use futures_util::stream::StreamExt;
use opendal::services;
use opendal::Operator;
use tracing::{error, info};

#[derive(Parser)]
struct Cli {
    /// Filter images by prefix
    #[arg(long)]
    prefix: Option<String>,

    /// S3 bucket name
    #[arg(long, env = "S3_BUCKET")]
    bucket: String,

    /// S3 region
    #[arg(long, env = "S3_REGION")]
    region: String,

    /// S3 endpoint
    #[arg(long, env = "S3_ENDPOINT")]
    endpoint: String,

    /// S3 access key id
    #[arg(long, env = "S3_ACCESS_KEY_ID")]
    access_key_id: String,

    /// S3 secret access key
    #[arg(long, env = "S3_SECRET_ACCESS_KEY")]
    secret_access_key: String,
}

struct ImageArchiver {
    docker: Docker,
    storage: opendal::Operator,
}

impl ImageArchiver {
    pub fn new(docker: Docker, storage: opendal::Operator) -> Self {
        Self { docker, storage }
    }

    async fn upload_image(&self, image: bollard::models::ImageSummary) -> Result<()> {
        let image_tag = image
            .repo_tags
            .first()
            .context("Image has no tags")?
            .to_string();

        let path = format!(
            "{}_{}.tar",
            image_tag.replace(['/', ':'], "_"),
            image.id.strip_prefix("sha256:").unwrap()
        );
        if self
            .storage
            .exists(&path)
            .await
            .context("Failed to check file existence")?
        {
            info!("File {} already exists, skipping", path);
            return Ok(());
        }

        info!("Exporting docker image {}", image_tag);
        let mut tar_stream = self.docker.export_image(&image_tag);
        let mut writer = self
            .storage
            .writer(&path)
            .await
            .context("Failed to create writer")?;

        info!("Uploading image {} to {}", image_tag, path);
        let pb = indicatif::ProgressBar::new_spinner().with_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})",
            )?,
        );

        let result = {
            while let Some(chunk) = tar_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        let len = bytes.len();
                        writer.write(bytes).await.context("Failed to write chunk")?;
                        pb.inc(len as u64);
                    }
                    Err(e) => {
                        pb.finish_and_clear();
                        return Err(e).context("Failed to read tar stream");
                    }
                }
            }
            pb.finish_and_clear();
            Ok(())
        };

        match result {
            Ok(_) => {
                writer.close().await.context("Failed to close writer")?;
                info!("Successfully uploaded {}", image_tag);
                Ok(())
            }
            Err(e) => {
                writer.abort().await?;
                Err(e)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let docker = Docker::connect_with_local_defaults()
        .context("Failed to connect to Docker")?
        .with_timeout(Duration::from_secs(600));

    let builder = services::S3::default()
        .bucket(&cli.bucket)
        .region(&cli.region)
        .root("/")
        .disable_ec2_metadata()
        .endpoint(&cli.endpoint)
        .access_key_id(&cli.access_key_id)
        .secret_access_key(&cli.secret_access_key);

    let storage = Operator::new(builder)?.finish();

    let archiver = ImageArchiver::new(docker.clone(), storage);

    let images = docker
        .list_images::<String>(None)
        .await
        .context("Failed to list Docker images")?;

    let filtered_images = images
        .into_iter()
        .filter(|image| {
            !image.repo_tags.is_empty() && !image.repo_tags.contains(&"<none>:<none>".to_string())
        })
        .filter(|image| {
            cli.prefix.as_ref().map_or(true, |prefix| {
                image.repo_tags.iter().any(|tag| tag.starts_with(prefix))
            })
        });

    for image in filtered_images {
        if let Err(e) = archiver.upload_image(image).await {
            error!("Failed to upload image: {:#}", e);
        }
    }

    Ok(())
}
