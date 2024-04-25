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

use std::{sync::Arc, time::Duration};

use tokio::net::ToSocketAddrs;

use crate::OneOrMany;

use super::Result;

pub trait Service: Sized + Send + Sync + 'static {
    type Request: bincode::Decode + Send + Sync;
    type RequestRef<'a>: bincode::Encode + Send + Sync;
    type Response: bincode::Encode + bincode::Decode + Send + Sync;

    fn handle(
        req: Self::Request,
        server: &Self,
    ) -> impl std::future::Future<Output = Self::Response> + Send + '_;
}

pub trait Message<S: Service> {
    type Response;
    fn handle(self, server: &S) -> impl std::future::Future<Output = Self::Response>;
}
pub trait Wrapper<S: Service>: Message<S> {
    fn wrap_request_ref(req: &Self) -> S::RequestRef<'_>;
    fn unwrap_response(res: S::Response) -> Option<Self::Response>;
}

pub struct Server<S: Service> {
    inner: super::Server<OneOrMany<S::Request>, OneOrMany<S::Response>>,
    service: Arc<S>,
}

impl<S: Service> Server<S> {
    pub async fn bind(service: S, addr: impl ToSocketAddrs) -> Result<Self> {
        Ok(Server {
            inner: super::Server::bind(addr).await?,
            service: Arc::new(service),
        })
    }
    pub async fn accept(&self) -> Result<()> {
        let mut req = self.inner.accept().await?;

        let service = Arc::clone(&self.service);
        tokio::spawn(async move {
            match req.take_body() {
                OneOrMany::One(body) => {
                    let res = S::handle(body, &service).await;

                    if let Err(e) = req.respond(OneOrMany::One(res)).await {
                        tracing::error!("failed to respond to request: {}", e);
                    }
                }
                OneOrMany::Many(bodies) => {
                    let mut res = Vec::new();

                    for req in bodies {
                        res.push(S::handle(req, &service).await);
                    }

                    if let Err(e) = req.respond(OneOrMany::Many(res)).await {
                        tracing::error!("failed to respond to request: {}", e);
                    }
                }
            }
        });

        Ok(())
    }
}

pub struct Connection<'a, S: Service> {
    inner: super::Connection<OneOrMany<S::RequestRef<'a>>, OneOrMany<S::Response>>,
}

impl<'a, S: Service> Connection<'a, S> {
    pub async fn create(server: impl ToSocketAddrs) -> Result<Connection<'a, S>> {
        Ok(Connection {
            inner: super::Connection::create(server).await?,
        })
    }

    pub async fn create_with_timeout(
        server: impl ToSocketAddrs,
        timeout: Duration,
    ) -> Result<Connection<'a, S>> {
        Ok(Connection {
            inner: super::Connection::create_with_timeout(server, timeout).await?,
        })
    }

    pub async fn create_with_timeout_retry(
        server: impl ToSocketAddrs + Clone,
        timeout: Duration,
        retry: impl Iterator<Item = Duration>,
    ) -> Result<Connection<'a, S>> {
        Ok(Connection {
            inner: super::Connection::create_with_timeout_retry(server, timeout, retry).await?,
        })
    }

    pub async fn send_without_timeout<R: Wrapper<S>>(self, request: &'a R) -> Result<R::Response> {
        Ok(R::unwrap_response(
            self.inner
                .send_without_timeout(&OneOrMany::One(R::wrap_request_ref(request)))
                .await?
                .one()
                .expect("response is missing"),
        )
        .unwrap())
    }

    pub async fn send<R: Wrapper<S>>(self, request: &'a R) -> Result<R::Response> {
        Ok(R::unwrap_response(
            self.inner
                .send(&OneOrMany::One(R::wrap_request_ref(request)))
                .await?
                .one()
                .expect("response is missing"),
        )
        .unwrap())
    }

    pub async fn send_with_timeout<R: Wrapper<S>>(
        self,
        request: &'a R,
        timeout: Duration,
    ) -> Result<R::Response> {
        Ok(R::unwrap_response(
            self.inner
                .send_with_timeout(&OneOrMany::One(R::wrap_request_ref(request)), timeout)
                .await?
                .one()
                .expect("response is missing"),
        )
        .unwrap())
    }

    pub async fn batch_send_with_timeout<R: Wrapper<S>>(
        self,
        requests: &'a [R],
        timeout: Duration,
    ) -> Result<Vec<R::Response>> {
        Ok(self
            .inner
            .send_with_timeout(
                &OneOrMany::Many(
                    requests
                        .iter()
                        .map(|req| R::wrap_request_ref(req))
                        .collect::<Vec<_>>(),
                ),
                timeout,
            )
            .await?
            .many()
            .into_iter()
            .map(|res| R::unwrap_response(res).unwrap())
            .collect())
    }
}

macro_rules! sonic_service {
    ($service:ident, [$($req:ident),*$(,)?]) => {
        mod service_impl__ {
            #![allow(dead_code)]

            use super::{$service, $($req),*};

            use $crate::distributed::sonic;

            #[derive(Debug, Clone, ::bincode::Decode)]
            pub enum Request {
                $($req(Box<$req>),)*
            }
            #[derive(Debug, Clone, ::bincode::Encode)]
            pub enum RequestRef<'a> {
                $($req(&'a $req),)*
            }
            #[derive(::bincode::Encode, ::bincode::Decode)]
            pub enum Response {
                $($req(Box<<$req as sonic::service::Message<$service>>::Response>),)*
            }
            $(
                impl sonic::service::Wrapper<$service> for $req {
                    fn wrap_request_ref(req: &Self) -> RequestRef {
                        RequestRef::$req(req)
                    }
                    fn unwrap_response(res: <$service as sonic::service::Service>::Response) -> Option<Self::Response> {
                        #[allow(irrefutable_let_patterns)]
                        if let Response::$req(value) = res {
                            Some(*value)
                        } else {
                            None
                        }
                    }
                }
            )*
            impl sonic::service::Service for $service {
                type Request = Request;
                type RequestRef<'a> = RequestRef<'a>;
                type Response = Response;

                // NOTE: This is a workaround for the fact that async functions
                // don't have a Send bound by default, and there's currently no
                // way of specifying that.
                #[allow(clippy::manual_async_fn)]
                fn handle(req: Request, server: &Self) -> impl std::future::Future<Output = Self::Response> + Send + '_ {
                    async move {
                        match req {
                            $(
                                Request::$req(value) => Response::$req(Box::new(sonic::service::Message::handle(*value, server).await)),
                            )*
                        }
                    }
                }
            }
            impl $service {
                pub async fn bind(self, addr: impl ::tokio::net::ToSocketAddrs) -> sonic::Result<sonic::service::Server<Self>> {
                    sonic::service::Server::bind(self, addr).await
                }
            }
        }
    };
}

pub(crate) use sonic_service;

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use std::{marker::PhantomData, net::SocketAddr, sync::atomic::AtomicI32};

    use super::{Server, Service, Wrapper};
    use futures::Future;

    struct ConnectionBuilder<S> {
        addr: SocketAddr,
        marker: PhantomData<S>,
    }

    impl<S: Service> ConnectionBuilder<S> {
        async fn send<R: Wrapper<S>>(&self, req: &R) -> Result<R::Response, anyhow::Error> {
            Ok(super::Connection::create(self.addr)
                .await?
                .send(req)
                .await?)
        }
    }

    fn fixture<
        S: Service + Send + Sync + 'static,
        B: Send + Sync + 'static,
        Y: Future<Output = Result<B, TestCaseError>> + Send,
    >(
        service: S,
        con_fn: impl FnOnce(ConnectionBuilder<S>) -> Y + Send + 'static,
    ) -> Result<B, TestCaseError>
    where
        S::Request: Send + Sync + 'static,
        S::Response: Send + Sync + 'static,
    {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let server = Server::bind(service, ("127.0.0.1", 0)).await.unwrap();
                let addr = server.inner.listener.local_addr().unwrap();

                let svr_task: tokio::task::JoinHandle<Result<(), anyhow::Error>> =
                    tokio::spawn(async move {
                        loop {
                            server.accept().await?;
                        }
                    });
                let con_res = tokio::spawn(async move {
                    con_fn(ConnectionBuilder {
                        addr,
                        marker: PhantomData,
                    })
                    .await
                })
                .await;
                svr_task.abort();

                con_res.unwrap_or_else(|err| panic!("connection failed: {err}"))
            })
    }

    mod counter_service {
        use std::sync::atomic::AtomicI32;

        use proptest_derive::Arbitrary;

        use super::super::Message;

        pub struct CounterService {
            pub counter: AtomicI32,
        }

        sonic_service!(CounterService, [Change, Reset]);

        #[derive(
            Debug,
            Clone,
            serde::Serialize,
            serde::Deserialize,
            bincode::Encode,
            bincode::Decode,
            Arbitrary,
        )]
        pub struct Change {
            pub amount: i32,
        }
        #[derive(
            Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
        )]
        pub struct Reset;

        impl Message<CounterService> for Change {
            type Response = i32;

            async fn handle(self, server: &CounterService) -> Self::Response {
                let prev = server
                    .counter
                    .fetch_add(self.amount, std::sync::atomic::Ordering::SeqCst);
                prev + self.amount
            }
        }

        impl Message<CounterService> for Reset {
            type Response = ();

            async fn handle(self, server: &CounterService) -> Self::Response {
                server.counter.store(0, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    use counter_service::*;

    #[test]
    fn simple_service() -> Result<(), TestCaseError> {
        fixture(
            CounterService {
                counter: AtomicI32::new(0),
            },
            |b| async move {
                let val = b
                    .send(&Change { amount: 15 })
                    .await
                    .map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                assert_eq!(val, 15);
                let val = b
                    .send(&Change { amount: 15 })
                    .await
                    .map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                assert_eq!(val, 30);
                b.send(&Reset)
                    .await
                    .map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                let val = b
                    .send(&Change { amount: 15 })
                    .await
                    .map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                assert_eq!(val, 15);
                Ok(())
            },
        )?;

        Ok(())
    }

    proptest! {
        #[test]
        fn ref_serialization(a: Change) {
            fixture(CounterService { counter: AtomicI32::new(0) }, |conn| async move {
                conn.send(&Reset).await.map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                let val = conn.send(&a).await.map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                prop_assert_eq!(val, a.amount);
                Ok(())
            })?;
        }
    }
}
