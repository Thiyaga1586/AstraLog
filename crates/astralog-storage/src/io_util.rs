use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{Result, StorageError};

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        // AstraLog's production durability contract is Linux-first. Windows is
        // supported for development/tests, where std does not expose a portable
        // directory fsync primitive.
        let _ = path;
    }
    Ok(())
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or(StorageError::InvalidFormat(
        "atomic-write path has no parent",
    ))?;
    fs::create_dir_all(parent)?;
    let temp = temp_path(path)?;

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }

    fs::rename(&temp, path)?;
    sync_directory(parent)?;
    Ok(())
}

pub(crate) fn create_new_durable(path: &Path, bytes: &[u8]) -> Result<File> {
    let parent = path
        .parent()
        .ok_or(StorageError::InvalidFormat("new-file path has no parent"))?;
    fs::create_dir_all(parent)?;
    if path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("durable file already exists: {}", path.display()),
        )
        .into());
    }
    let temp = temp_path(path)?;
    match fs::remove_file(&temp) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }

    if path.exists() {
        let _ = fs::remove_file(&temp);
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("durable file appeared while staging: {}", path.display()),
        )
        .into());
    }
    fs::rename(&temp, path)?;
    sync_directory(parent)?;

    let file = OpenOptions::new().read(true).write(true).open(path)?;
    Ok(file)
}

fn temp_path(path: &Path) -> Result<PathBuf> {
    let name =
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or(StorageError::InvalidFormat(
                "path file name is not valid UTF-8",
            ))?;
    Ok(path.with_file_name(format!("{name}.tmp")))
}
