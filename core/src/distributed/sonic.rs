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

use std::{net::SocketAddr, time::Duration};

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
    IO(#[from] std::io::Error),

    #[error("Error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),

    #[error("Failed to connect to peer: connection timeout")]
    ConnectionTimeout,
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
        let bytes = bincode::serialize(&response).unwrap();
        let header = Header {
            body_size: bytes.len(),
        };

        self.stream
            .write_all(unsafe { any_as_u8_slice(&header) })
            .await?;
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

        let body = bincode::deserialize(&buf).unwrap();

        Ok(Request { stream, body })
    }
}

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub async fn create(server: impl ToSocketAddrs) -> Result<Self> {
        Self::create_with_timeout(server, Duration::from_secs(30)).await
    }

    pub async fn create_with_timeout(
        server: impl ToSocketAddrs,
        timeout: Duration,
    ) -> Result<Self> {
        match tokio::time::timeout(timeout, TcpStream::connect(server)).await {
            Ok(stream) => {
                let stream = stream?;
                Ok(Connection { stream })
            }
            Err(_) => Err(Error::ConnectionTimeout),
        }
    }

    pub async fn send_without_timeout<T: Serialize, R: DeserializeOwned + Serialize>(
        mut self,
        request: &T,
    ) -> Result<Response<R>> {
        let bytes = bincode::serialize(&request).unwrap();

        let header = Header {
            body_size: bytes.len(),
        };

        self.stream
            .write_all(unsafe { any_as_u8_slice(&header) })
            .await?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        let mut header_buf = vec![0; std::mem::size_of::<Header>()];
        self.stream.read_exact(&mut header_buf).await?;
        let header: Header = unsafe { std::ptr::read(header_buf.as_ptr() as *const _) };

        let mut buf = vec![0; header.body_size];
        self.stream.read_exact(&mut buf).await?;

        self.stream.shutdown().await?;

        Ok(bincode::deserialize(&buf).unwrap())
    }

    pub async fn send<T: Serialize, R: DeserializeOwned + Serialize>(
        self,
        request: &T,
    ) -> Result<Response<R>> {
        self.send_with_timeout(request, Duration::from_secs(30))
            .await
    }

    pub async fn send_with_timeout<T: Serialize, R: DeserializeOwned + Serialize>(
        self,
        request: &T,
        timeout: Duration,
    ) -> Result<Response<R>> {
        match tokio::time::timeout(timeout, self.send_without_timeout(request)).await {
            Ok(res) => res,
            Err(_) => Err(Error::ConnectionTimeout),
        }
    }
}

pub struct ResilientConnection<Rt: Iterator<Item = Duration>> {
    addr: SocketAddr,
    retry: Rt,
}

impl<Rt: Iterator<Item = Duration>> ResilientConnection<Rt> {
    pub fn create(addr: SocketAddr, retry: Rt) -> Self {
        Self { addr, retry }
    }

    pub async fn send_with_timeout<T: Serialize, R: DeserializeOwned + Serialize>(
        &mut self,
        request: &T,
        timeout: Duration,
    ) -> Result<Response<R>> {
        loop {
            match Connection::create_with_timeout(&self.addr, timeout).await {
                Ok(conn) => {
                    let response = conn.send_with_timeout(request, timeout).await;
                    if let Err(Error::ConnectionTimeout) = response {
                        continue;
                    }
                    return response;
                }
                Err(Error::ConnectionTimeout) => {
                    if let Some(timeout) = self.retry.next() {
                        tokio::time::sleep(timeout).await;
                        continue;
                    } else {
                        return Err(Error::ConnectionTimeout);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}
