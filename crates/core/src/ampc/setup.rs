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
// along with this program.  If not, see <https://www.gnu.org/license

use super::DhtConn;

/// A setup is responsible for initializing the DHT before each round of computation.
pub trait Setup {
    type DhtTables;

    /// Setup initial state of the DHT.
    fn init_dht(&self) -> DhtConn<Self::DhtTables>;

    /// Setup state for a new round.
    ///
    /// This is called once for each round of computation.
    /// The first round will run `setup_first_round` first
    /// but will still call `setup_round` after that.
    #[allow(unused_variables)] // reason = "dht might be used by implementors"
    fn setup_round(&self, dht: &Self::DhtTables) {}

    /// Setup state for the first round.
    ///
    /// This is called once before the first round of computation.
    fn setup_first_round(&self, dht: &Self::DhtTables) {
        self.setup_round(dht);
    }
}
