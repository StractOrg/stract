// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::net::SocketAddr;

mod manager;
mod worker;

pub use manager::Manager;
use thiserror::Error;
pub use worker::Worker;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IO(#[from] std::io::Error),

    #[error("error while serializing/deserializing to/from bytes")]
    Serialization(#[from] bincode::Error),

    #[error("could not get a working worker")]
    NoAvailableWorker,

    #[error("failed to get value from channel")]
    ChannelRecv(#[from] async_channel::RecvError),

    #[error("did not get a reponse")]
    NoResponse,
}

pub trait Map<T>
where
    Self: Serialize + DeserializeOwned + Send,
    T: Serialize + DeserializeOwned + Send,
{
    fn map(self) -> T;
}

pub trait Reduce<T = Self>
where
    Self: Serialize + DeserializeOwned,
{
    fn reduce(self, element: T) -> T;
}

pub trait MapReduce<I, O>
where
    Self: Sized + Iterator<Item = I> + Send,
    I: Map<O>,
    O: Reduce<O> + Send,
{
    fn map_reduce(self, workers: &[SocketAddr]) -> Option<O> {
        let manager = Manager::new(workers);
        manager.run(self)
    }
}
impl<I, O, T> MapReduce<I, O> for T
where
    T: Iterator<Item = I> + Sized + Send,
    I: Map<O>,
    O: Reduce<O> + Send,
{
}

#[derive(Serialize, Deserialize, Debug)]
enum Task<T> {
    Job(T),
    AllFinished,
}
