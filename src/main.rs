use anyhow::{Context, Result};
use bollard::Docker;
use clap::Parser;
use opendal::services;
use opendal::Operator;
use tracing::{error, info};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Filter images by prefix
    #[arg(long)]
    prefix: Option<String>,

    /// S3 bucket name
    #[arg(long, env = "S3_BUCKET")]
    bucket: String,

    /// S3 region
    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    region: String,
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
        use futures_util::stream::StreamExt;

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
        let pb = indicatif::ProgressBar::new_spinner();
        pb.set_style(indicatif::ProgressStyle::default_spinner().template(
            "{spinner:.green} [{elapsed_precise}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )?);

        let result = {
            while let Some(chunk) = tar_stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        let len = bytes.len();
                        dbg!(len);
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
    // Initialize logging
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Initialize Docker client
    let docker = Docker::connect_with_local_defaults().context("Failed to connect to Docker")?;

    // Initialize S3 storage
    let builder = services::S3::default()
        .bucket(&cli.bucket)
        .region(&cli.region);

    let builder = builder
        .root("/")
        .endpoint("http://172.17.0.1:9000")
        .bucket(&cli.bucket)
        .access_key_id("minioadmin")
        .secret_access_key("minioadmin")
        .region(&cli.region)
        .disable_ec2_metadata()
        .disable_stat_with_override();

    let storage = Operator::new(builder)?.finish();

    let archiver = ImageArchiver::new(docker.clone(), storage);

    // List and filter images
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

    // Upload images
    for image in filtered_images {
        if let Err(e) = archiver.upload_image(image).await {
            error!("Failed to upload image: {:#}", e);
        }
    }

    Ok(())
}
