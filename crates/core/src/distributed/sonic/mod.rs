// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

pub mod connection_pool;
pub mod replication;
pub mod service;

pub use connection_pool::ConnectionPool;

use std::{marker::PhantomData, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

const MAX_BODY_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1TB
const MAX_CONNECTION_TTL: Duration = Duration::from_secs(60);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Got an IO error")]
    IO(#[from] std::io::Error),

    #[error("Failed to connect to peer: connection timeout")]
    ConnectionTimeout,

    #[error("Failed to get response for request: connection timeout")]
    RequestTimeout,

    #[error("Could not get connection from pool")]
    PoolGet,

    #[error("The request could not be processed")]
    BadRequest,

    #[error("The body size ({body_size}) is larger than the maximum allowed ({max_size})")]
    BodyTooLarge { body_size: usize, max_size: usize },

    #[error("An application error occurred: {0}")]
    Application(#[from] anyhow::Error),
}

pub struct Connection<Req, Res> {
    stream: TcpStream,
    created: std::time::Instant,
    marker: PhantomData<(Req, Res)>,
    awaiting_res: bool,
}

impl<Req, Res> Connection<Req, Res>
where
    Req: bincode::Encode,
    Res: bincode::Decode,
{
    pub async fn connect(server: impl ToSocketAddrs) -> Result<Self> {
        Self::create(server).await
    }

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
                stream.set_nodelay(true)?;

                Ok(Connection {
                    stream,
                    awaiting_res: false,
                    created: std::time::Instant::now(),
                    marker: PhantomData,
                })
            }
            Err(_) => Err(Error::ConnectionTimeout),
        }
    }

    pub async fn create_with_timeout_retry(
        server: impl ToSocketAddrs + Clone,
        timeout: Duration,
        retry: impl Iterator<Item = Duration>,
    ) -> Result<Self> {
        let mut conn = Connection::create_with_timeout(server.clone(), timeout).await;
        let mut retry = retry;

        loop {
            match conn {
                Ok(conn) => return Ok(conn),
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

    async fn send_without_timeout(&mut self, request: &Req) -> Result<Res> {
        self.awaiting_res = true;
        let bytes = bincode::encode_to_vec(request, bincode::config::standard()).unwrap();

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
            return Err(Error::BodyTooLarge {
                body_size: header.body_size,
                max_size: MAX_BODY_SIZE_BYTES,
            });
        }

        let mut buf = vec![0; header.body_size];
        self.stream.read_exact(&mut buf).await?;
        self.stream.flush().await?;

        tracing::debug!("deserializing {:?}", std::any::type_name::<(Req, Res)>());
        let (res, _) = bincode::decode_from_slice(&buf, bincode::config::standard()).unwrap();

        self.awaiting_res = false;

        Ok(res)
    }

    pub async fn send(&mut self, request: &Req) -> Result<Res> {
        self.send_with_timeout(request, Duration::from_secs(90))
            .await
    }

    pub async fn send_with_timeout(&mut self, request: &Req, timeout: Duration) -> Result<Res> {
        match tokio::time::timeout(timeout, self.send_without_timeout(request)).await {
            Ok(res) => res,
            Err(_) => {
                self.stream.shutdown().await?;
                Err(Error::RequestTimeout)
            }
        }
    }

    pub fn awaiting_response(&self) -> bool {
        self.awaiting_res
    }

    pub async fn is_closed(&mut self) -> bool {
        if self.created.elapsed() > MAX_CONNECTION_TTL {
            self.stream.shutdown().await.ok();
            return true;
        }

        !matches!(
            tokio::time::timeout(Duration::from_secs(1), self.stream.read_exact(&mut [])).await,
            Ok(Ok(_))
        )
    }
}

#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
struct Header {
    body_size: usize,
}

pub struct Server<Req, Res> {
    listener: TcpListener,
    marker: PhantomData<(Req, Res)>,
}

impl<Req, Res> Server<Req, Res>
where
    Req: bincode::Decode,
{
    pub async fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Server {
            listener,
            marker: PhantomData,
        })
    }

    pub async fn accept(&self) -> Result<ServerConnection<Req, Res>> {
        let (stream, client) = self.listener.accept().await?;
        tracing::debug!(?client, "accepted connection");

        Ok(ServerConnection::new(stream))
    }
}

pub struct ServerConnection<Req, Res> {
    stream: TcpStream,
    marker: PhantomData<(Req, Res)>,
}

impl<Req, Res> ServerConnection<Req, Res>
where
    Req: bincode::Decode,
{
    fn new(stream: TcpStream) -> Self {
        ServerConnection {
            stream,
            marker: PhantomData,
        }
    }

    pub async fn request(&mut self) -> Result<Request<'_, Req, Res>> {
        let mut header_buf = vec![0; std::mem::size_of::<Header>()];
        self.stream.read_exact(&mut header_buf).await?;
        let header: Header = *bytemuck::from_bytes(&header_buf);

        if header.body_size > MAX_BODY_SIZE_BYTES {
            return Err(Error::BodyTooLarge {
                body_size: header.body_size,
                max_size: MAX_BODY_SIZE_BYTES,
            });
        }

        let mut buf = vec![0; header.body_size];

        self.stream.read_exact(&mut buf).await?;

        let (body, _) = bincode::decode_from_slice(&buf, bincode::config::standard()).unwrap();

        Ok(Request {
            conn: self,
            body: Some(body),
        })
    }
}

pub struct Request<'a, Req, Res> {
    conn: &'a mut ServerConnection<Req, Res>,
    body: Option<Req>,
}

impl<'a, Req, Res> Request<'a, Req, Res>
where
    Res: bincode::Encode,
{
    async fn respond_without_timeout(self, response: Res) -> Result<()> {
        let bytes = bincode::encode_to_vec(&response, bincode::config::standard()).unwrap();
        let header = Header {
            body_size: bytes.len(),
        };

        self.conn
            .stream
            .write_all(bytemuck::bytes_of(&header))
            .await?;
        self.conn.stream.write_all(&bytes).await?;
        self.conn.stream.flush().await?;

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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, future::Future};

    use proptest::prelude::*;

    use crate::free_socket_addr;

    use super::*;

    fn fixture<
        Req: bincode::Encode + bincode::Decode + Send + 'static,
        Res: bincode::Encode + bincode::Decode + Send + 'static,
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
                let addr = free_socket_addr();
                let server = Server::bind(addr).await.unwrap();
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

    #[derive(Debug, Clone, bincode::Encode, bincode::Decode, PartialEq)]
    struct Message {
        text: String,
        other: HashMap<String, f32>,
    }

    impl Arbitrary for Message {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: ()) -> Self::Strategy {
            (
                any::<String>(),
                prop::collection::hash_map(".*", 0.0f32..100.0f32, 0..10),
            )
                .prop_map(|(text, other)| Message { text, other })
                .boxed()
        }
    }

    proptest! {
        #[test]
        fn basic_arb(a1: Message, b1: Message) {
            let (a2, b2) = (a1.clone(), b1.clone());
            let (svr_res, con_res) = fixture(
                |svr| async move {
                    let mut conn = svr.accept().await?;
                    let req = conn.request().await?;
                    prop_assert_eq!(req.body(), &a1);
                    req.respond(b1).await?;
                    Ok(())
                },
                |mut con| async move {
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
