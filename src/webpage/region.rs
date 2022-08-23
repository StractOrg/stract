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

use crate::{Error, Result};

#[derive(PartialEq, Eq)]
pub enum Region {
    All,
    Denmark,
    France,
    Germany,
    Spain,
    US,
}

pub const ALL_REGIONS: [Region; 6] = [
    Region::All,
    Region::Denmark,
    Region::France,
    Region::Germany,
    Region::Spain,
    Region::US,
];

impl Region {
    pub fn name(&self) -> String {
        match self {
            Region::All => "All Regions".to_string(),
            Region::Denmark => "Denmark".to_string(),
            Region::France => "France".to_string(),
            Region::Germany => "Germany".to_string(),
            Region::Spain => "Spain".to_string(),
            Region::US => "United States".to_string(),
        }
    }

    pub fn gl(&self) -> String {
        match self {
            Region::All => "all".to_string(),
            Region::Denmark => "dk".to_string(),
            Region::France => "fr".to_string(),
            Region::Germany => "ger".to_string(),
            Region::Spain => "spa".to_string(),
            Region::US => "us".to_string(),
        }
    }

    pub fn from_gl(gl: &str) -> Result<Self> {
        match gl {
            "all" => Ok(Region::All),
            "dk" => Ok(Region::Denmark),
            "fr" => Ok(Region::France),
            "ger" => Ok(Region::Germany),
            "spa" => Ok(Region::Spain),
            "us" => Ok(Region::US),
            _ => Err(Error::UnknownRegion),
        }
    }
}
