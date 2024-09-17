// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use crate::Result;
use std::{
    fs,
    future::Future,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const CHUNK_SIZE_BYTES: usize = 1024 * 1024; // 1 MB

pub trait Stepper {
    fn step(&self, req: Request) -> impl Future<Output = Response>;
}

impl<F> Stepper for F
where
    F: Fn(Request) -> Response,
{
    async fn step(&self, req: Request) -> Response {
        self(req)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum Request {
    Overview { root: PathBuf },
    Chunk { path: PathBuf, offset: u64 },
}

impl Request {
    pub async fn download<P1, P2, S>(remote_path: P1, to: P2, stepper: &S)
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
        S: Stepper,
    {
        let Response::Overview { paths } = stepper
            .step(Self::Overview {
                root: remote_path.as_ref().to_path_buf(),
            })
            .await
        else {
            panic!("unexpected response to request")
        };

        for path in paths {
            let local = to.as_ref().join(path.clone());
            if let Some(parent) = local.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent).expect("failed to create directory");
                }
            }

            Self::download_file(remote_path.as_ref().join(path.clone()), local, stepper).await;
        }
    }

    pub async fn download_file<P1, P2, S>(remote: P1, local: P2, stepper: &S)
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
        S: Stepper,
    {
        let mut offset = 0;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(local.as_ref())
            .unwrap_or_else(|_| panic!("failed to create file {:?}", local.as_ref().as_os_str()));
        let mut writer = BufWriter::new(file);

        loop {
            let Response::Chunk { bytes } = stepper
                .step(Self::Chunk {
                    path: remote.as_ref().to_path_buf(),
                    offset,
                })
                .await
            else {
                panic!("unexpected response to request");
            };

            writer.write_all(&bytes).unwrap();

            if bytes.len() < CHUNK_SIZE_BYTES {
                break;
            }

            offset += bytes.len() as u64;
        }
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum Response {
    Overview { paths: Vec<PathBuf> },
    Chunk { bytes: Vec<u8> },
}

impl Response {
    pub fn handle(req: Request) -> Result<Self> {
        match req {
            Request::Overview { root } => Ok(Response::Overview {
                paths: files(root.clone())
                    .into_iter()
                    .filter_map(|p| p.strip_prefix(&root).ok().map(|p| p.to_path_buf()))
                    .collect(),
            }),
            Request::Chunk { path, offset } => {
                let file = fs::File::open(path)?;
                let mut reader = BufReader::new(file);

                reader.seek(SeekFrom::Start(offset)).ok();

                let mut res = Vec::with_capacity(CHUNK_SIZE_BYTES);
                let mut buf = [0; 512];

                while res.len() < CHUNK_SIZE_BYTES {
                    let n = reader.read(&mut buf)?;

                    if n == 0 {
                        break;
                    }

                    res.extend_from_slice(&buf[..n]);
                }

                Ok(Response::Chunk { bytes: res })
            }
        }
    }
}

fn files(root: PathBuf) -> Vec<PathBuf> {
    let mut stack = vec![root];
    let mut res = Vec::new();

    while let Some(path) = stack.pop() {
        if path.is_file() {
            res.push(path);
        } else if let Ok(dir) = fs::read_dir(path) {
            for entry in dir.flatten() {
                stack.push(entry.path());
            }
        }
    }

    res
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use file_store::gen_temp_path;

    use super::*;

    fn local_step(req: Request) -> Response {
        Response::handle(req).unwrap()
    }

    async fn download<P1: AsRef<Path>, P2: AsRef<Path>>(remote: P1, local: P2) {
        Request::download(remote, local, &local_step).await
    }

    #[tokio::test]
    async fn test_directory() {
        let a = gen_temp_path();
        std::fs::create_dir_all(a.join("test")).unwrap();
        let contents = "this is a test";
        std::fs::write(a.join("test").join("file.txt"), contents).unwrap();

        let b = gen_temp_path();
        download(&a, &b).await;

        assert!(b.join("test").exists());
        assert!(b.join("test").join("file.txt").exists());

        let res = std::fs::read_to_string(b.join("test").join("file.txt")).unwrap();

        assert_eq!(contents, res);
    }

    #[tokio::test]
    async fn test_single_file() {
        let a = gen_temp_path();
        std::fs::create_dir_all(&a).unwrap();
        let contents = "this is a test";
        std::fs::write(a.join("file.txt"), contents).unwrap();

        let b = gen_temp_path();
        download(&a, &b).await;

        assert!(b.join("file.txt").exists());

        let res = std::fs::read_to_string(b.join("file.txt")).unwrap();

        assert_eq!(contents, res);
    }

    #[tokio::test]
    async fn test_overwrite() {
        let a = gen_temp_path();
        std::fs::create_dir_all(&a).unwrap();
        let contents = "this is a test";
        std::fs::write(a.join("file.txt"), contents).unwrap();

        let b = gen_temp_path();
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(b.join("file.txt"), "this is another test").unwrap();
        download(&a, &b).await;

        assert!(b.join("file.txt").exists());

        let res = std::fs::read_to_string(b.join("file.txt")).unwrap();

        assert_eq!(contents, res);
    }

    #[tokio::test]
    async fn test_keep_non_copied() {
        let a = gen_temp_path();
        std::fs::create_dir_all(a.join("test")).unwrap();
        let contents = "this is a test";
        std::fs::write(a.join("test").join("a.txt"), contents).unwrap();

        let b = gen_temp_path();
        std::fs::create_dir_all(b.join("test")).unwrap();
        std::fs::write(b.join("test").join("b.txt"), contents).unwrap();

        download(&a, &b).await;

        assert!(b.join("test").exists());
        assert!(b.join("test").join("a.txt").exists());
        assert!(b.join("test").join("b.txt").exists());

        let res = std::fs::read_to_string(b.join("test").join("a.txt")).unwrap();
        assert_eq!(contents, res);

        let res = std::fs::read_to_string(b.join("test").join("b.txt")).unwrap();
        assert_eq!(contents, res);
    }

    #[tokio::test]
    async fn test_file_size_edge_case() {
        let content = "a".repeat(CHUNK_SIZE_BYTES - 1);
        let a = gen_temp_path();
        std::fs::create_dir_all(&a).unwrap();
        std::fs::write(a.join("minus_1.txt"), &content).unwrap();
        std::fs::write(a.join("edge.txt"), format!("{}a", &content)).unwrap();
        std::fs::write(a.join("plus_1.txt"), format!("{}aa", &content)).unwrap();

        let b = gen_temp_path();
        download(&a, &b).await;
        assert!(b.join("minus_1.txt").exists());
        assert!(b.join("edge.txt").exists());
        assert!(b.join("plus_1.txt").exists());

        let res = std::fs::read_to_string(b.join("minus_1.txt")).unwrap();
        assert_eq!(content, res);

        let res = std::fs::read_to_string(b.join("edge.txt")).unwrap();
        assert_eq!(format!("{}a", &content), res);

        let res = std::fs::read_to_string(b.join("plus_1.txt")).unwrap();
        assert_eq!(format!("{}aa", &content), res);

        std::fs::remove_dir_all(&a).unwrap();
        std::fs::remove_dir_all(&b).unwrap();
    }
}
