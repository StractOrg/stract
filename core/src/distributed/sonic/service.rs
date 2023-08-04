use std::{marker::PhantomData, net::SocketAddr, time::Duration};

use tokio::net::ToSocketAddrs;

use super::{Error, Result};

#[async_trait::async_trait]
pub trait Service: Sized {
    type Request: serde::de::DeserializeOwned;
    type RequestRef<'a>: serde::Serialize;
    type Response: serde::Serialize + serde::de::DeserializeOwned;

    async fn handle(req: Self::Request, server: &mut Self) -> Result<Self::Response>;
}

pub struct Connection<'a, S: Service> {
    inner: super::Connection<S::RequestRef<'a>, S::Response>,
}

#[async_trait::async_trait]
pub trait Message<S: Service> {
    type Response;
    async fn handle(self, server: &mut S) -> Result<Self::Response>;
}
pub trait Wrapper<S: Service>: Message<S> {
    fn wrap_request_ref(req: &Self) -> S::RequestRef<'_>;
    fn unwrap_response(res: S::Response) -> Option<Self::Response>;
}

pub struct Server<S: Service> {
    inner: super::Server<S::Request, S::Response>,
    service: S,
}

pub struct ResilientConnection<S: Service, Rt: Iterator<Item = Duration>> {
    addr: SocketAddr,
    retry: Rt,
    marker: PhantomData<S>,
}

impl<S: Service> Server<S> {
    pub async fn bind(service: S, addr: impl ToSocketAddrs) -> Result<Self> {
        Ok(Server {
            inner: super::Server::bind(addr).await?,
            service,
        })
    }
    pub async fn accept(&mut self) -> Result<()> {
        let mut req = self.inner.accept().await?;
        let res = S::handle(req.take_body(), &mut self.service).await?;
        req.respond(res).await
    }
}

impl<'a, S: Service> Connection<'a, S> {
    #[allow(dead_code)]
    pub async fn create(server: impl ToSocketAddrs) -> Result<Connection<'a, S>> {
        Ok(Connection {
            inner: super::Connection::create(server).await?,
        })
    }
    #[allow(dead_code)]
    pub async fn create_with_timeout(
        server: impl ToSocketAddrs,
        timeout: Duration,
    ) -> Result<Connection<'a, S>> {
        Ok(Connection {
            inner: super::Connection::create_with_timeout(server, timeout).await?,
        })
    }
    #[allow(dead_code)]
    pub async fn send_without_timeout<R: Wrapper<S>>(self, request: &'a R) -> Result<R::Response> {
        Ok(R::unwrap_response(
            self.inner
                .send_without_timeout(&R::wrap_request_ref(request))
                .await?,
        )
        .unwrap())
    }
    #[allow(dead_code)]
    pub async fn send<R: Wrapper<S>>(self, request: &'a R) -> Result<R::Response> {
        Ok(R::unwrap_response(self.inner.send(&R::wrap_request_ref(request)).await?).unwrap())
    }
    #[allow(dead_code)]
    pub async fn send_with_timeout<R: Wrapper<S>>(
        self,
        request: &'a R,
        timeout: Duration,
    ) -> Result<R::Response> {
        Ok(R::unwrap_response(
            self.inner
                .send_with_timeout(&R::wrap_request_ref(request), timeout)
                .await?,
        )
        .unwrap())
    }
}

impl<S: Service, Rt: Iterator<Item = Duration>> ResilientConnection<S, Rt> {
    pub fn create(addr: SocketAddr, retry: Rt) -> Self {
        Self {
            addr,
            retry,
            marker: PhantomData,
        }
    }

    pub async fn send_with_timeout<R: Wrapper<S>>(
        mut self,
        request: &R,
        timeout: Duration,
    ) -> Result<R::Response> {
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

#[macro_export]
macro_rules! sonic_service {
    ($service:ident, [$($req:ident),*$(,)?]) => {
        mod service_impl__ {
            #![allow(dead_code)]

            use super::{$service, $($req),*};

            use $crate::distributed::sonic;

            #[derive(Debug, Clone, ::serde::Deserialize)]
            pub enum Request {
                $($req($req),)*
            }
            #[derive(Debug, Clone, ::serde::Serialize)]
            pub enum RequestRef<'a> {
                $($req(&'a $req),)*
            }
            #[derive(::serde::Serialize, ::serde::Deserialize)]
            pub enum Response {
                $($req(<$req as sonic::service::Message<$service>>::Response),)*
            }
            $(
                impl sonic::service::Wrapper<$service> for $req {
                    fn wrap_request_ref(req: &Self) -> RequestRef {
                        RequestRef::$req(req)
                    }
                    fn unwrap_response(res: <$service as sonic::service::Service>::Response) -> Option<Self::Response> {
                        #[allow(irrefutable_let_patterns)]
                        if let Response::$req(value) = res {
                            Some(value)
                        } else {
                            None
                        }
                    }
                }
            )*
            #[async_trait::async_trait]
            impl sonic::service::Service for $service {
                type Request = Request;
                type RequestRef<'a> = RequestRef<'a>;
                type Response = Response;

                async fn handle(req: Request, server: &mut Self) -> sonic::Result<Response> {
                    match req {
                        $(
                            Request::$req(value) => Ok(Response::$req(sonic::service::Message::handle(value, server).await?)),
                        )*
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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use std::{marker::PhantomData, net::SocketAddr};

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
                let mut server = Server::bind(service, ("127.0.0.1", 0)).await.unwrap();
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
        use proptest_derive::Arbitrary;
        use serde::{Deserialize, Serialize};

        use crate::distributed::sonic;

        use super::super::Message;

        pub struct CounterService {
            pub counter: i32,
        }

        sonic_service!(CounterService, [Change, Reset]);

        #[derive(Debug, Clone, Serialize, Deserialize, Arbitrary)]
        pub struct Change {
            pub amount: i32,
        }
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct Reset;

        #[async_trait::async_trait]
        impl Message<CounterService> for Change {
            type Response = i32;

            async fn handle(self, server: &mut CounterService) -> sonic::Result<Self::Response> {
                server.counter += self.amount;
                Ok(server.counter)
            }
        }

        #[async_trait::async_trait]
        impl Message<CounterService> for Reset {
            type Response = ();

            async fn handle(self, server: &mut CounterService) -> sonic::Result<Self::Response> {
                server.counter = 0;
                Ok(())
            }
        }
    }

    use counter_service::*;

    #[test]
    fn simple_service() -> Result<(), TestCaseError> {
        fixture(CounterService { counter: 0 }, |b| async move {
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
        })?;

        Ok(())
    }

    proptest! {
        #[test]
        fn ref_serialization(a: Change) {
            fixture(CounterService { counter: 0 }, |conn| async move {
                conn.send(&Reset).await.map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                let val = conn.send(&a).await.map_err(|e| TestCaseError::Fail(e.to_string().into()))?;
                prop_assert_eq!(val, a.amount);
                Ok(())
            })?;
        }
    }
}
