use std::net::SocketAddr;

use super::Result;
use super::{Map, Task};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, info};

pub struct Worker {}

enum State {
    Continue,
    Finished,
}

impl Worker {
    async fn run_stream<I, O, S>(mut stream: S) -> Result<State>
    where
        I: Map<O>,
        O: Serialize + DeserializeOwned + Send,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut buf = [0; 4096];
        let mut bytes = Vec::new();
        loop {
            if let Ok(size) = stream.read(&mut buf).await {
                debug!("read {:?} bytes", size);
                bytes.extend_from_slice(&buf);
                if size < buf.len() {
                    break;
                }
            }
        }

        match bincode::deserialize::<Task<I>>(&bytes)? {
            Task::Job(job) => {
                debug!("received job");
                let res = job.map();
                let bytes = bincode::serialize(&res)?;
                stream.write_all(&bytes[..]).await?;
            }
            Task::AllFinished => {
                debug!("shutting down");
                return Ok(State::Finished);
            }
        };

        Ok(State::Continue)
    }

    pub async fn run<I, O>(addr: SocketAddr) -> Result<()>
    where
        I: Map<O>,
        O: Serialize + DeserializeOwned + Send,
    {
        let listener = TcpListener::bind(addr).await?;
        info!("worker listening on: {:}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            debug!("received connection");
            match Worker::run_stream::<I, O, _>(socket).await? {
                State::Finished => break,
                State::Continue => {}
            }
        }

        Ok(())
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
        ) -> Poll<std::result::Result<usize, std::io::Error>> {
            self.get_mut().result = Vec::from(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), std::io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), std::io::Error>> {
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
        Worker::run_stream::<MockJob, _, _>(&mut stream)
            .await
            .expect("worker failed");

        let res: Count = bincode::deserialize(&stream.result).unwrap();

        assert_eq!(res.0, 3);
    }
}
