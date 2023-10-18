pub mod image_downloader;
pub mod image_store;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("image was not of format: {format:?}")]
    InvalidImageFormat {
        source: image::ImageError,
        format: image::ImageFormat,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
