// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Got an IO error")]
    IOError(#[from] std::io::Error),

    #[error("Error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),
}

#[derive(Serialize, Deserialize)]
pub enum Response<T: Serialize> {
    Empty,
    Content(T),
}

#[repr(C)]
struct Header {
    body_size: usize,
}

pub struct Request<T> {
    stream: TcpStream,
    pub body: T,
}

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    std::slice::from_raw_parts((p as *const T) as *const u8, std::mem::size_of::<T>())
}

impl<T> Request<T> {
    pub async fn respond<R: Serialize>(mut self, response: Response<R>) -> Result<()> {
        let bytes = bincode::serialize(&response)?;
        let header = Header {
            body_size: bytes.len(),
        };

        self.stream
            .write_all(unsafe { any_as_u8_slice(&header) })
            .await?;
        self.stream.flush().await?;

        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        self.stream.shutdown().await?;

        Ok(())
    }
}

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }

    pub async fn accept<T>(&self) -> Result<Request<T>>
    where
        T: Serialize + DeserializeOwned,
    {
        let (mut stream, client) = self.listener.accept().await?;
        tracing::debug!("accepted connection from: {}", &client);

        let mut header_buf = vec![0; std::mem::size_of::<Header>()];
        stream.read_exact(&mut header_buf).await?;
        let header: Header = unsafe { std::ptr::read(header_buf.as_ptr() as *const _) };

        let mut buf = vec![0; header.body_size];

        stream.read_exact(&mut buf).await?;
        tracing::debug!("received bytes: {:?}", &buf);

        let body = bincode::deserialize(&buf)?;

        Ok(Request { stream, body })
    }
}

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub async fn create(server: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(server).await?;

        Ok(Connection { stream })
    }

    pub async fn send<T: Serialize, R: DeserializeOwned + Serialize>(
        mut self,
        request: T,
    ) -> Result<Response<R>> {
        let bytes = bincode::serialize(&request)?;

        let header = Header {
            body_size: bytes.len(),
        };

        self.stream
            .write_all(unsafe { any_as_u8_slice(&header) })
            .await?;
        self.stream.flush().await?;

        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        let mut header_buf = vec![0; std::mem::size_of::<Header>()];
        self.stream.read_exact(&mut header_buf).await?;
        let header: Header = unsafe { std::ptr::read(header_buf.as_ptr() as *const _) };

        let mut buf = vec![0; header.body_size];
        self.stream.read_exact(&mut buf).await?;

        self.stream.shutdown().await?;

        Ok(bincode::deserialize(&buf)?)
    }
}

// let server = sonic::Server::new();
// loop {
//     let job = server.accept();
//
//     job.respond(respons)
// }
//
//
//
//
// let connection = sonic::connect("123.123.123.123:1337");
// let res = connection.send(job);
