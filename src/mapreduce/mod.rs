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

use std::net::SocketAddr;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

mod manager;
mod worker;

pub use manager::Manager;
pub use worker::Worker;

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

#[async_trait]
pub trait MapReduce<I, O>: Iterator<Item = I>
where
    Self: Sized,
    I: Map<O>,
    O: Reduce<O> + Send,
{
    async fn map_reduce(self, workers: &[SocketAddr]) -> Option<O> {
        let manager = Manager::new(workers);
        manager.run(self.collect()).await
    }
}
impl<I: Map<O> + Send, O: Reduce<O> + Send, T: Sized> MapReduce<I, O> for T where
    T: Iterator<Item = I>
{
}

#[derive(Serialize, Deserialize, Debug)]
enum Task<T> {
    Job(T),
    AllFinished,
}
