use std::path::PathBuf;

/// A directory lock.
///
/// A lock is associated with a specific path.
///
/// The lock will be passed to [`Directory::acquire_lock`](crate::Directory::acquire_lock).
#[derive(Debug)]
pub struct Lock {
    /// The lock needs to be associated with its own file `path`.
    /// Depending on the platform, the lock might rely on the creation
    /// and deletion of this filepath.
    pub filepath: PathBuf,
    /// `is_blocking` describes whether acquiring the lock is meant
    /// to be a blocking operation or a non-blocking.
    ///
    /// Acquiring a blocking lock blocks until the lock is
    /// available.
    ///
    /// Acquiring a non-blocking lock returns rapidly, either successfully
    /// or with an error signifying that someone is already holding
    /// the lock.
    pub is_blocking: bool,
}
