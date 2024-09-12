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
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const CHUNK_SIZE_BYTES: usize = 1024 * 1024 * 1024; // 1 MB

enum Request {
    Overview { root: PathBuf },
    Chunk { path: PathBuf, offset: u64 },
}

impl Request {
    pub fn download<P1, P2, F>(remote_path: P1, to: P2, step: F)
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
        F: Fn(Self) -> Response,
    {
        let Response::Overview { paths } = step(Self::Overview {
            root: remote_path.as_ref().to_path_buf(),
        }) else {
            panic!("unexpected response to request")
        };

        for path in paths {
            let local = to.as_ref().join(path.clone());
            if let Some(parent) = local.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent).expect("failed to create directory");
                }
            }

            Self::download_file(remote_path.as_ref().join(path.clone()), local, &step);
        }
    }

    pub fn download_file<P1, P2, F>(remote: P1, local: P2, step: F)
    where
        P1: AsRef<Path>,
        P2: AsRef<Path>,
        F: Fn(Self) -> Response,
    {
        let mut offset = 0;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(local.as_ref())
            .expect(&format!(
                "failed to create file {:?}",
                local.as_ref().as_os_str()
            ));
        let mut writer = BufWriter::new(file);

        loop {
            let Response::Chunk { bytes } = step(Self::Chunk {
                path: remote.as_ref().to_path_buf(),
                offset,
            }) else {
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

enum Response {
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
        } else {
            if let Ok(dir) = fs::read_dir(path) {
                for entry in dir {
                    if let Ok(entry) = entry {
                        stack.push(entry.path());
                    }
                }
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

    fn download<P1: AsRef<Path>, P2: AsRef<Path>>(remote: P1, local: P2) {
        Request::download(remote, local, local_step)
    }

    #[test]
    fn test_directory() {
        let a = gen_temp_path();
        std::fs::create_dir_all(a.join("test")).unwrap();
        let contents = "this is a test";
        std::fs::write(a.join("test").join("file.txt"), contents).unwrap();

        let b = gen_temp_path();
        download(&a, &b);

        assert!(b.join("test").exists());
        assert!(b.join("test").join("file.txt").exists());

        let res = std::fs::read_to_string(b.join("test").join("file.txt")).unwrap();

        assert_eq!(contents, res);
    }

    #[test]
    fn test_single_file() {
        let a = gen_temp_path();
        std::fs::create_dir_all(&a).unwrap();
        let contents = "this is a test";
        std::fs::write(a.join("file.txt"), contents).unwrap();

        let b = gen_temp_path();
        download(&a, &b);

        assert!(b.join("file.txt").exists());

        let res = std::fs::read_to_string(b.join("file.txt")).unwrap();

        assert_eq!(contents, res);
    }
}
