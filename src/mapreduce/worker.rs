use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
};

use crate::mapreduce::manager::{BUF_SIZE, END_OF_MESSAGE};

use super::{Error, Map, Result, Task};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info};

pub struct Worker {}

enum State {
    Continue,
    Finished,
}

impl Worker {
    fn run_stream<I, O, S>(mut stream: S) -> Result<State>
    where
        I: Map<O>,
        O: Serialize + DeserializeOwned + Send,
        S: Read + Write + Unpin,
    {
        let mut buf = [0; BUF_SIZE];
        let mut bytes = Vec::new();
        loop {
            if let Ok(size) = stream.read(&mut buf) {
                debug!("read {:?} bytes", size);
                if size == 0 && bytes.is_empty() {
                    return Err(Error::NoResponse);
                }

                bytes.extend_from_slice(&buf[..size]);

                if bytes.len() >= END_OF_MESSAGE.len()
                    && bytes[bytes.len() - END_OF_MESSAGE.len()..] == END_OF_MESSAGE
                {
                    break;
                }
            }
        }

        bytes = bytes[..bytes.len() - END_OF_MESSAGE.len()].to_vec();

        match bincode::deserialize::<Task<I>>(&bytes)? {
            Task::Job(job) => {
                debug!("received job");
                let res = job.map();
                let bytes = bincode::serialize(&res)?;
                debug!("serialized result into {} bytes", bytes.len());
                stream.write_all(&bytes)?;
                stream.write_all(&END_OF_MESSAGE)?;
            }
            Task::AllFinished => {
                debug!("shutting down");
                return Ok(State::Finished);
            }
        };

        Ok(State::Continue)
    }

    pub fn run<I, O>(addr: SocketAddr) -> Result<()>
    where
        I: Map<O>,
        O: Serialize + DeserializeOwned + Send,
    {
        let listener = TcpListener::bind(addr)?;
        info!("worker listening on: {:}", addr);

        loop {
            let (socket, _) = listener.accept()?;
            debug!("received connection");
            match Worker::run_stream::<I, O, _>(socket)? {
                State::Finished => break,
                State::Continue => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    struct MockTcpStream {
        contents: Vec<u8>,
        result: Vec<u8>,
        num_read: usize,
    }

    impl MockTcpStream {
        fn new(contents: Vec<u8>) -> Self {
            Self {
                contents,
                num_read: 0,
                result: Vec::new(),
            }
        }
    }

    impl Read for MockTcpStream {
        fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
            if self.num_read == 0 {
                buf.write_all(&self.contents[..]).unwrap();
            } else if self.num_read == 1 {
                buf.write_all(&END_OF_MESSAGE).unwrap();
            }

            self.contents = Vec::new();
            self.num_read += 1;
            Ok(buf.len())
        }
    }

    impl Write for MockTcpStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.result.extend(Vec::from(buf));
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
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

    #[test]
    fn execute() {
        let contents = vec![1, 2, 0, 1, 0, 1, 0];
        let job = bincode::serialize(&Task::Job(MockJob { contents })).unwrap();

        let mut stream = MockTcpStream::new(job);
        Worker::run_stream::<MockJob, _, _>(&mut stream).expect("worker failed");

        let result_bytes = &stream.result[..stream.result.len() - END_OF_MESSAGE.len()];
        let res: Count = bincode::deserialize(result_bytes).unwrap();

        assert_eq!(res.0, 3);
    }
}
