use std::net::SocketAddr;

use super::{Map, Task};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, ToSocketAddrs};
use tracing::{debug, info};

pub struct Worker {}

impl Worker {
    async fn run_stream<I, O, S>(mut stream: S)
    where
        I: Map<O>,
        O: Serialize + DeserializeOwned + Send,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut buf = vec![0; 4096];
        loop {
            if let Ok(size) = stream.read(&mut buf).await {
                if size == 0 {
                    break;
                }
                debug!("read {:?} bytes", size);
                match bincode::deserialize::<Task<I>>(&buf[..size]).unwrap() {
                    Task::Job(job) => {
                        debug!("received job");
                        let res = job.map();
                        let bytes = bincode::serialize(&res).unwrap();
                        stream.write_all(&bytes[..]).await.unwrap();
                    }
                    Task::AllFinished => break,
                };
            }
        }
    }

    pub async fn run<I, O>(addr: SocketAddr)
    where
        I: Map<O>,
        O: Serialize + DeserializeOwned + Send,
    {
        let listener = TcpListener::bind(addr).await.unwrap();
        info!("worker listening on: {:}", addr);

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            debug!("received connection");
            Worker::run_stream::<I, O, _>(socket).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use serde::Deserialize;
    use tokio::io::ReadBuf;

    use super::*;

    struct MockTcpStream {
        contents: Vec<u8>,
        result: Vec<u8>,
    }

    impl MockTcpStream {
        fn new(contents: Vec<u8>) -> Self {
            Self {
                contents,
                result: Vec::new(),
            }
        }
    }

    impl AsyncRead for MockTcpStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            buf.put_slice(&self.contents[..]);
            self.get_mut().contents = Vec::new();
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MockTcpStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            self.get_mut().result = Vec::from(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct MockJob {
        contents: Vec<usize>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Count(usize);

    impl Map<Count> for MockJob {
        fn map(self) -> Count {
            Count(self.contents.into_iter().filter(|d| *d == 0).count())
        }
    }

    #[tokio::test]
    async fn execute() {
        let job = bincode::serialize(&Task::Job(MockJob {
            contents: vec![1, 2, 0, 1, 0, 1, 0],
        }))
        .unwrap();

        let mut stream = MockTcpStream::new(job);
        Worker::run_stream::<MockJob, _, _>(&mut stream).await;

        let res: Count = bincode::deserialize(&stream.result).unwrap();

        assert_eq!(res.0, 3);
    }
}
