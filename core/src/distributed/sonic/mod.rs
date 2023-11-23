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

pub mod replication;
pub mod service;

use std::{marker::PhantomData, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

const MAX_BODY_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1TB

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Got an IO error")]
    IO(#[from] std::io::Error),

    #[error("Error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),

    #[error("Failed to connect to peer: connection timeout")]
    ConnectionTimeout,

    #[error("Failed to get response for request: connection timeout")]
    RequestTimeout,

    #[error("Could not build connection pool")]
    PoolCreation,

    #[error("Other")]
    Other(#[from] anyhow::Error),
}

pub struct Connection<Req, Res> {
    stream: TcpStream,
    marker: PhantomData<(Req, Res)>,
}

impl<Req, Res> Connection<Req, Res>
where
    Req: Serialize,
    Res: DeserializeOwned,
{
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
                Ok(Connection {
                    stream,
                    marker: PhantomData,
                })
            }
            Err(_) => Err(Error::ConnectionTimeout),
        }
    }

    async fn send_without_timeout(mut self, request: &Req) -> Result<Res> {
        let bytes = bincode::serialize(&request).unwrap();

        // disable linger to avoid TIME_WAIT.
        // should be safe since the connection is closed from the client side.
        // No stray packets should therefore find its way to the socket.
        self.stream.set_linger(Some(Duration::from_secs(0)))?;

        let header = Header {
            body_size: bytes.len(),
        };

        self.stream.write_all(bytemuck::bytes_of(&header)).await?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        let mut header_buf = vec![0; std::mem::size_of::<Header>()];
        self.stream.read_exact(&mut header_buf).await?;
        let header: Header = *bytemuck::from_bytes(&header_buf);

        if header.body_size > MAX_BODY_SIZE_BYTES {
            return Err(Error::Other(anyhow::anyhow!(
                "body size too large: {} (max: {})",
                header.body_size,
                MAX_BODY_SIZE_BYTES
            )));
        }

        let mut buf = vec![0; header.body_size];
        self.stream.read_exact(&mut buf).await?;
        self.stream.flush().await?;
        self.stream.shutdown().await?;

        tracing::debug!("deserializing {:?}", std::any::type_name::<(Req, Res)>());
        Ok(bincode::deserialize(&buf).unwrap())
    }

    pub async fn send(self, request: &Req) -> Result<Res> {
        self.send_with_timeout(request, Duration::from_secs(90))
            .await
    }

    pub async fn send_with_timeout(self, request: &Req, timeout: Duration) -> Result<Res> {
        match tokio::time::timeout(timeout, self.send_without_timeout(request)).await {
            Ok(res) => res,
            Err(_) => Err(Error::RequestTimeout),
        }
    }
}

#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
struct Header {
    body_size: usize,
}

pub struct Server<Req, Res> {
    pub(super) listener: TcpListener,
    marker: PhantomData<(Req, Res)>,
}

impl<Req, Res> Server<Req, Res>
where
    Req: DeserializeOwned,
{
    pub async fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Server {
            listener,
            marker: PhantomData,
        })
    }

    async fn parse_incoming_stream(&self, mut stream: TcpStream) -> Result<Request<Req, Res>> {
        let mut header_buf = vec![0; std::mem::size_of::<Header>()];
        stream.read_exact(&mut header_buf).await?;
        let header: Header = *bytemuck::from_bytes(&header_buf);

        if header.body_size > MAX_BODY_SIZE_BYTES {
            return Err(Error::Other(anyhow::anyhow!(
                "body size too large: {} (max: {})",
                header.body_size,
                MAX_BODY_SIZE_BYTES
            )));
        }

        let mut buf = vec![0; header.body_size];

        stream.read_exact(&mut buf).await?;

        let body = Some(bincode::deserialize(&buf).unwrap());

        Ok(Request {
            stream,
            body,
            marker: PhantomData,
        })
    }

    pub async fn accept(&self) -> Result<Request<Req, Res>> {
        let (stream, client) = self.listener.accept().await?;
        tracing::debug!(?client, "accepted connection");

        tokio::time::timeout(Duration::from_secs(60), self.parse_incoming_stream(stream))
            .await
            .map_err(|_| Error::ConnectionTimeout)?
    }
}

pub struct Request<Req, Res> {
    stream: TcpStream,
    body: Option<Req>,
    marker: PhantomData<(Req, Res)>,
}

impl<Req, Res> Request<Req, Res>
where
    Res: Serialize,
{
    async fn respond_without_timeout(mut self, response: Res) -> Result<()> {
        let bytes = bincode::serialize(&response).unwrap();
        let header = Header {
            body_size: bytes.len(),
        };

        self.stream.write_all(bytemuck::bytes_of(&header)).await?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        // wait for client to close connection
        let mut buf: [u8; 1] = [0];
        tokio::time::timeout(Duration::from_secs(5), self.stream.read_exact(&mut buf))
            .await
            .ok();

        Ok(())
    }

    pub async fn respond(self, response: Res) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(90),
            self.respond_without_timeout(response),
        )
        .await
        .map_err(|_| Error::RequestTimeout)?
    }

    pub fn body(&self) -> &Req {
        self.body.as_ref().unwrap()
    }

    fn take_body(&mut self) -> Req {
        self.body.take().expect("body was taken twice")
    }
}

pub struct ResilientConnection<Req, Res> {
    conn: Connection<Req, Res>,
}

impl<Req, Res> ResilientConnection<Req, Res>
where
    Req: Serialize,
    Res: DeserializeOwned,
{
    pub async fn create_with_timeout(
        server: impl ToSocketAddrs + Clone,
        timeout: Duration,
        retry: impl Iterator<Item = Duration>,
    ) -> Result<Self> {
        let mut conn = Connection::create_with_timeout(server.clone(), timeout).await;
        let mut retry = retry;

        loop {
            match conn {
                Ok(conn) => return Ok(ResilientConnection { conn }),
                Err(_) => {
                    if let Some(timeout) = retry.next() {
                        tokio::time::sleep(timeout).await;
                        conn = Connection::create_with_timeout(server.clone(), timeout).await;
                    } else {
                        return Err(Error::ConnectionTimeout);
                    }
                }
            }
        }
    }

    pub async fn send_with_timeout(self, request: &Req, timeout: Duration) -> Result<Res> {
        self.conn.send_with_timeout(request, timeout).await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, future::Future};

    use proptest::prelude::*;
    use proptest_derive::Arbitrary;
    use serde::Deserialize;

    use super::*;

    fn fixture<
        Req: Serialize + DeserializeOwned + Send + 'static,
        Res: Serialize + DeserializeOwned + Send + 'static,
        A: Send + 'static,
        B: Send + 'static,
        X: Future<Output = Result<A, TestCaseError>> + Send,
        Y: Future<Output = Result<B, TestCaseError>> + Send,
    >(
        svr_fn: impl FnOnce(Server<Req, Res>) -> X + Send + 'static,
        con_fn: impl FnOnce(Connection<Req, Res>) -> Y + Send + 'static,
    ) -> (Result<A, TestCaseError>, Result<B, TestCaseError>) {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let server = Server::bind(("127.0.0.1", 0)).await.unwrap();
                let addr = server.listener.local_addr().unwrap();
                let connection = Connection::create(addr).await.unwrap();

                let svr_task = tokio::spawn(async move { svr_fn(server).await });
                let con_task = tokio::spawn(async move { con_fn(connection).await });

                let (svr_res, con_res) = tokio::join!(svr_task, con_task);
                (
                    svr_res.unwrap_or_else(|err| panic!("server failed: {err}")),
                    con_res.unwrap_or_else(|err| panic!("connection failed: {err}")),
                )
            })
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Arbitrary)]
    struct Message {
        text: String,
        other: HashMap<String, f32>,
    }

    proptest! {
        #[test]
        fn basic_arb(a1: Message, b1: Message) {
            let (a2, b2) = (a1.clone(), b1.clone());
            let (svr_res, con_res) = fixture(
                |svr| async move {
                    let req = svr.accept().await?;
                    prop_assert_eq!(req.body(), &a1);
                    req.respond(b1).await?;
                    Ok(())
                },
                |con| async move {
                    let res = con.send(&a2).await?;
                    prop_assert_eq!(res, b2);
                    Ok(())
                },
            );
            svr_res?;
            con_res?;
        }
    }
}
