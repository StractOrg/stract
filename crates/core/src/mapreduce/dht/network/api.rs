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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use crate::distributed::sonic;

use super::NetworkConnection;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Set {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Get {
    pub key: String,
}

impl sonic::service::Message<NetworkConnection> for Set {
    type Response = ();

    async fn handle(self, server: &NetworkConnection) -> sonic::Result<Self::Response> {
        let res = server
            .raft
            .client_write(self.into())
            .await
            .map_err(|e| sonic::Error::Application(e.into()))?;

        Ok(res.data.try_into().unwrap())
    }
}

impl sonic::service::Message<NetworkConnection> for Get {
    type Response = Option<String>;

    async fn handle(self, server: &NetworkConnection) -> sonic::Result<Self::Response> {
        Ok(server
            .state_machine_store
            .state_machine
            .read()
            .await
            .data
            .get(&self.key)
            .cloned())
    }
}
