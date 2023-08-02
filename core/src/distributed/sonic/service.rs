use std::{marker::PhantomData, net::SocketAddr, time::Duration};

use tokio::net::ToSocketAddrs;

use super::{Error, Result};

#[async_trait::async_trait]
pub trait Service: Sized {
    type Request: serde::Serialize + serde::de::DeserializeOwned;
    type Response: serde::Serialize + serde::de::DeserializeOwned;

    async fn handle(req: Self::Request, server: &mut Self) -> Result<Self::Response>;
}

#[async_trait::async_trait]
pub trait Message<S: Service> {
    type Response;
    async fn handle(self, server: &mut S) -> Result<Self::Response>;
}
pub trait Wrapper<S: Service>: Message<S> {
    fn wrap_request(req: Self) -> S::Request;
    fn unwrap_response(res: S::Response) -> Option<Self::Response>;
}

pub struct Server<S: Service> {
    inner: super::Server<S::Request, S::Response>,
    service: S,
}

pub struct Connection<S: Service> {
    inner: super::Connection<S::Request, S::Response>,
    marker: PhantomData<S>,
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

impl<S: Service> Connection<S> {
    #[allow(dead_code)]
    pub async fn create(server: impl ToSocketAddrs) -> Result<Self> {
        Ok(Connection {
            inner: super::Connection::create(server).await?,
            marker: PhantomData,
        })
    }

    pub async fn create_with_timeout(
        server: impl ToSocketAddrs,
        timeout: Duration,
    ) -> Result<Self> {
        Ok(Connection {
            inner: super::Connection::create_with_timeout(server, timeout).await?,
            marker: PhantomData,
        })
    }

    #[allow(dead_code)]
    pub async fn send_without_timeout<R: Wrapper<S>>(self, request: R) -> Result<R::Response> {
        let res = self
            .inner
            .send_without_timeout(&R::wrap_request(request))
            .await?;
        Ok(R::unwrap_response(res).unwrap())
    }

    #[allow(dead_code)]
    pub async fn send<R: Wrapper<S>>(self, request: R) -> Result<R::Response> {
        let res = self.inner.send(&R::wrap_request(request)).await?;
        Ok(R::unwrap_response(res).unwrap())
    }

    pub async fn send_with_timeout<R: Wrapper<S>>(
        self,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response> {
        let res = self
            .inner
            .send_with_timeout(&R::wrap_request(request), timeout)
            .await?;
        Ok(R::unwrap_response(res).unwrap())
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
        request: R,
        timeout: Duration,
    ) -> Result<R::Response>
    where
        R: Clone,
    {
        loop {
            match Connection::create_with_timeout(&self.addr, timeout).await {
                Ok(conn) => {
                    let response = conn.send_with_timeout(request.clone(), timeout).await;
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

            #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
            pub enum Request {
                $($req($req),)*
            }
            #[derive(::serde::Serialize, ::serde::Deserialize)]
            pub enum Response {
                $($req(<$req as sonic::service::Message<$service>>::Response),)*
            }
            $(
                impl sonic::service::Wrapper<$service> for $req {
                    fn wrap_request(req: Self) -> Request{
                        Request::$req(req)
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
    use std::{marker::PhantomData, net::SocketAddr};

    use super::{Connection, Server, Service, Wrapper};
    use futures::Future;

    struct ConnectionBuilder<S> {
        addr: SocketAddr,
        marker: PhantomData<S>,
    }

    impl<S: Service> ConnectionBuilder<S> {
        async fn conn(&self) -> Result<Connection<S>, anyhow::Error> {
            Ok(Connection::create(self.addr).await?)
        }
        async fn send<R: Wrapper<S>>(&self, req: R) -> Result<R::Response, anyhow::Error> {
            Ok(self.conn().await?.send(req).await?)
        }
    }

    fn fixture<
        S: Service + Send + Sync + 'static,
        B: Send + Sync + 'static,
        Y: Future<Output = Result<B, anyhow::Error>> + Send,
    >(
        service: S,
        con_fn: impl FnOnce(ConnectionBuilder<S>) -> Y + Send + 'static,
    ) -> Result<B, anyhow::Error>
    where
        S::Request: Send + Sync + 'static + Clone,
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
        use serde::{Deserialize, Serialize};

        use crate::distributed::sonic;

        use super::super::Message;

        pub struct CounterService {
            pub counter: i32,
        }

        sonic_service!(CounterService, [Change, Reset]);

        #[derive(Debug, Clone, Serialize, Deserialize)]
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

    #[test]
    fn simple_service() -> Result<(), anyhow::Error> {
        use counter_service::*;

        fixture(CounterService { counter: 0 }, |b| async move {
            let val = b.send(Change { amount: 15 }).await?;
            assert_eq!(val, 15);
            let val = b.send(Change { amount: 15 }).await?;
            assert_eq!(val, 30);
            b.send(Reset).await?;
            let val = b.send(Change { amount: 15 }).await?;
            assert_eq!(val, 15);
            Ok(())
        })?;

        Ok(())
    }
}
