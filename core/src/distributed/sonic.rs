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

use bytemuck::{Pod, Zeroable};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Got an IO error")]
    IO(#[from] std::io::Error),

    #[error("Error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),

    #[error("Failed to connect to peer: connection timeout")]
    ConnectionTimeout,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Response<T: Serialize> {
    Empty,
    Content(T),
}

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct Header {
    body_size: usize,
}

pub struct Request<T> {
    stream: TcpStream,
    pub body: T,
}

impl<T> Request<T> {
    pub async fn respond<R: Serialize>(mut self, response: Response<R>) -> Result<()> {
        let bytes = bincode::serialize(&response).unwrap();
        let header = Header {
            body_size: bytes.len(),
        };

        self.stream.write_all(bytemuck::bytes_of(&header)).await?;
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
        let header: Header = *bytemuck::from_bytes(&header_buf);

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

        self.stream.write_all(bytemuck::bytes_of(&header)).await?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        let mut header_buf = vec![0; std::mem::size_of::<Header>()];
        self.stream.read_exact(&mut header_buf).await?;
        let header: Header = *bytemuck::from_bytes(&header_buf);

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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, future::Future};

    use proptest::prelude::*;
    use proptest_derive::Arbitrary;

    use super::*;

    fn fixture<
        A: Send + Sync + 'static,
        B: Send + Sync + 'static,
        X: Future<Output = Result<A, TestCaseError>> + Send,
        Y: Future<Output = Result<B, TestCaseError>> + Send,
    >(
        svr_fn: impl FnOnce(Server) -> X + Send + 'static,
        con_fn: impl FnOnce(Connection) -> Y + Send + 'static,
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
        fn basic_arb(msg_1: Message, msg_2: Message) {
            let (svr_res, con_res) = fixture(
                {
                    let msg_1 = msg_1.clone();
                    let msg_2 = msg_2.clone();
                    |svr| async move {
                        let req = svr.accept::<Message>().await?;

                        prop_assert_eq!(&req.body, &msg_1);

                        let res = Response::Content(msg_2);
                        req.respond(res).await?;

                        Ok(())
                    }
                },
                |con| async move {
                    let res: Response<Message> = con.send(&msg_1).await?;

                    prop_assert_eq!(res, Response::Content(msg_2));

                    Ok(())
                },
            );

            svr_res?;
            con_res?;
        }
    }
}
