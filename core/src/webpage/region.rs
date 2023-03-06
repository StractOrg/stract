// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use std::{collections::HashMap, fs::File, io::Write, path::Path};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

use super::Webpage;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, Serialize, Deserialize, Default)]
pub enum Region {
    #[default]
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

    pub fn id(&self) -> u64 {
        ALL_REGIONS
            .iter()
            .enumerate()
            .find(|(_, region)| self == *region)
            .map(|(id, _)| id as u64)
            .unwrap()
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

    pub fn guess_from(webpage: &Webpage) -> Result<Self> {
        match webpage.html.lang {
            Some(lang) => match lang {
                whatlang::Lang::Eng => Ok(Region::US),
                whatlang::Lang::Spa => Ok(Region::Spain),
                whatlang::Lang::Fra => Ok(Region::France),
                whatlang::Lang::Deu => Ok(Region::Germany),
                whatlang::Lang::Dan => Ok(Region::Denmark),
                _ => Err(Error::UnknownRegion),
            },
            None => Err(Error::UnknownRegion),
        }
    }

    pub fn from_id(doc: u64) -> Self {
        ALL_REGIONS[doc as usize]
    }

    pub fn lang(&self) -> Option<whatlang::Lang> {
        match self {
            Region::Denmark => Some(whatlang::Lang::Dan),
            Region::France => Some(whatlang::Lang::Fra),
            Region::Germany => Some(whatlang::Lang::Deu),
            Region::Spain => Some(whatlang::Lang::Spa),
            Region::US => Some(whatlang::Lang::Eng),
            Region::All => None,
        }
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct RegionCount {
    map: HashMap<Region, u64>,
    fast_count: Vec<Option<u64>>,
    total_counts: u64,
    path: String,
}

impl RegionCount {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let map: HashMap<Region, u64> = if !path.as_ref().exists() {
            if let Some(parent) = path.as_ref().parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            File::create(path.as_ref()).unwrap();

            HashMap::new()
        } else {
            let json = std::fs::read_to_string(path.as_ref()).unwrap_or_default();
            serde_json::from_str(&json).unwrap_or_else(|_| HashMap::new())
        };

        let mut fast_count = Vec::new();

        for (region, count) in &map {
            let idx = region.id() as usize;

            while idx >= fast_count.len() {
                fast_count.push(None);
            }

            fast_count[idx] = Some(*count);
        }

        RegionCount {
            total_counts: map.values().sum(),
            map,
            fast_count,
            path: path.as_ref().to_str().unwrap().to_string(),
        }
    }

    pub fn increment(&mut self, region: &Region) {
        let entry = self.map.entry(*region).or_insert(0);
        self.total_counts += 1;
        *entry += 1;
    }

    pub fn commit(&mut self) {
        let json = serde_json::to_string(&self.map).unwrap();
        let mut file = File::options().write(true).open(&self.path).unwrap();
        file.write_all(json.as_bytes()).unwrap();
        self.total_counts = self.map.values().sum();

        let mut fast_count = Vec::new();

        for (region, count) in &self.map {
            let idx = region.id() as usize;

            while idx >= fast_count.len() {
                fast_count.push(None);
            }

            fast_count[idx] = Some(*count);
        }

        self.fast_count = fast_count;
    }

    pub fn merge(&mut self, other: Self) {
        for (region, count) in other.map {
            *self.map.entry(region).or_insert(0) += count;
        }

        std::fs::remove_file(other.path).unwrap();

        self.commit()
    }

    pub fn score(&self, region: &Region) -> f64 {
        self.fast_count
            .get(region.id() as usize)
            .map(|count| match count {
                Some(count) => *count as f64 / self.total_counts as f64,
                None => 0.0,
            })
            .unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::gen_temp_path;

    use super::*;

    #[test]
    fn simple() {
        let mut a = RegionCount::open(gen_temp_path().join("region_count.json"));

        a.increment(&Region::Denmark);
        a.increment(&Region::Denmark);
        a.increment(&Region::US);

        let mut b = RegionCount::open(gen_temp_path().join("region_count.json"));

        b.increment(&Region::US);
        b.increment(&Region::Germany);

        a.merge(b);

        assert_eq!(a.map.get(&Region::Denmark), Some(&2));
        assert_eq!(a.map.get(&Region::US), Some(&2));
        assert_eq!(a.map.get(&Region::Germany), Some(&1));

        assert_eq!(a.score(&Region::Denmark), 0.4);
        assert_eq!(a.score(&Region::France), 0.0);
    }
}
