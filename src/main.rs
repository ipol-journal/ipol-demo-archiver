use std::time::Duration;

use anyhow::{Context, Result};
use bollard::Docker;
use clap::Parser;
use futures_util::stream::StreamExt;
use opendal::services;
use opendal::Operator;
use time::OffsetDateTime;
use tracing::{error, info};

#[derive(Parser)]
struct Cli {
    /// Filter images by prefix
    #[arg(long)]
    prefix: Option<String>,

    /// Ignore workshop demos
    #[arg(long)]
    ignore_workshop_demos: bool,

    /// Ignore test demos
    #[arg(long)]
    ignore_test_demos: bool,

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

#[derive(Debug, Clone)]
struct ImageSpec {
    /// starts with ipol-demo- (probably)
    name: String,
    /// git commit hash (it might not be a commit hash for non-demos images, that we still want to backup)
    commit_hash: String,
    /// docker image image id
    image_id: String,
    /// creation date of the docker image
    created: OffsetDateTime,
}

impl ImageSpec {
    fn as_tar_filename(&self) -> String {
        // if the name contains the registry, or any '_', replace by '-' so that the filename can
        // later be split in its three parts
        let name = self.name.replace(['/', '_'], "-");

        let image_hash = self.image_id.strip_prefix("sha256:").unwrap();

        format!("{}_{}_{}.tar", name, self.commit_hash, image_hash)
    }
}

impl TryFrom<&bollard::models::ImageSummary> for ImageSpec {
    type Error = anyhow::Error;

    fn try_from(value: &bollard::models::ImageSummary) -> Result<Self, Self::Error> {
        let image_id = value.id.clone();

        let image_tag = value
            .repo_tags
            .first()
            .context("image has no tags")?
            .to_string();

        let (name, commit_hash) = {
            let mut split = image_tag.split(':');

            let name = split
                .next()
                .context("invalid docker image tag, cannot split by `:`.")?
                .to_string();

            let commit_hash = split
                .next()
                .context("invalid docker image tag, cannot split by `:`.")?
                .to_string();

            anyhow::ensure!(
                split.next().is_none(),
                "invalid docker image tag, as more than one `:`."
            );
            (name, commit_hash)
        };

        let created = value.created;
        let created = OffsetDateTime::from_unix_timestamp(created)?;

        Ok(Self {
            name,
            commit_hash,
            image_id,
            created,
        })
    }
}

struct ImageArchiver {
    docker: Docker,
    storage: opendal::Operator,
}

impl ImageArchiver {
    pub fn new(docker: Docker, storage: opendal::Operator) -> Self {
        Self { docker, storage }
    }

    async fn upload_image(&self, image: bollard::models::ImageSummary, path: &str) -> Result<()> {
        if self
            .storage
            .exists(path)
            .await
            .context("Failed to check file existence")?
        {
            info!("File {} already exists, skipping", path);
            return Ok(());
        }

        let image_tag = image
            .repo_tags
            .first()
            .context("Image has no tags")?
            .to_string();

        let metadata = vec![("created".to_string(), image.created.to_string())];

        info!("Exporting docker image {:?}", image_tag);
        let mut tar_stream = self.docker.export_image(&image.id);
        let mut writer = self
            .storage
            .writer_with(path)
            .user_metadata(metadata)
            .chunk(128 * 1024 * 1024)
            .concurrent(8)
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

    async fn remove_existings(
        &self,
        prefix: &str,
        cutoff_date: OffsetDateTime,
    ) -> Result<Vec<String>> {
        let mut to_remove = vec![];
        let entries = self.storage.list(prefix).await?;
        for entry in entries {
            let key = entry.path();
            let date_put = entry
                .metadata()
                .last_modified()
                .context("no `last_modified` metadata?")?;
            let date_put: i64 = date_put.to_utc().timestamp();
            let date_put = OffsetDateTime::from_unix_timestamp(date_put)?;

            // If the upload is newer than the creation of the image,
            // it might be two cases:
            //   - an old image recently uploaded (which we want to replace)
            //   - an new image recently uploaded (which we don't want to replace)
            // But if the upload is older, then we definitely want to replace it
            if date_put >= cutoff_date {
                let metadata = self.storage.stat(key).await?;
                let user_metadata = metadata.user_metadata().context("no user_metadata")?;
                let actual_created = user_metadata
                    .get("created")
                    .context("no `created` field in user_metadata")?;
                let actual_created: i64 = actual_created.parse()?;
                let actual_created = OffsetDateTime::from_unix_timestamp(actual_created)?;

                if actual_created >= cutoff_date {
                    info!("skip {key} because it is newer (according to user metadata)");
                    continue;
                }
            }

            to_remove.push(key.to_string());
        }

        for key in &to_remove {
            info!("removing {key} because it is older than {cutoff_date}");
        }
        self.storage.delete_iter(to_remove.clone()).await?;

        Ok(to_remove)
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

    let prefix = cli.prefix.unwrap_or_default();
    for image in images {
        let Ok(imagespec) = ImageSpec::try_from(&image) else {
            info!("skip {} {:?}", image.id, image.repo_tags);
            continue;
        };

        if !imagespec.name.starts_with(&prefix) {
            continue;
        }

        if cli.ignore_workshop_demos && imagespec.name.starts_with("ipol-demo-77777") {
            continue;
        }

        if cli.ignore_test_demos && imagespec.name.starts_with("ipol-demo-55555") {
            continue;
        }

        let path = imagespec.as_tar_filename();

        info!("candidate for upload: {path}");
        let _existings = archiver.remove_existings(&path, imagespec.created).await?;
            if let Err(e) = archiver.upload_image(image, &path).await {
                error!("Failed to upload image: {:#}", e);
            }
        }
    }

    Ok(())
}
