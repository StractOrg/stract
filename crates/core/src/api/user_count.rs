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

use std::sync::Arc;

use anyhow::{anyhow, Result};
use chrono::{NaiveDateTime, Utc};
use ring::rand::SecureRandom;
use ring::{digest, pbkdf2, rand};
use std::sync::Mutex;

use crate::hyperloglog::HyperLogLog;
use crate::metrics::Counter;

pub trait Frequency: Clone + Copy + Default {
    fn next_reset(&self) -> NaiveDateTime;
}

#[derive(Clone, Copy, Default)]
pub struct Daily;
#[derive(Clone, Copy, Default)]
pub struct Monthly;

impl Frequency for Daily {
    fn next_reset(&self) -> NaiveDateTime {
        Utc::now()
            .date_naive()
            .succ_opt()
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }
}

impl Frequency for Monthly {
    fn next_reset(&self) -> NaiveDateTime {
        (Utc::now().date_naive() + chrono::Duration::days(31))
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }
}

struct InnerUserCount<F: Frequency> {
    frequency: F,
    metric: Counter,
    next_reset: NaiveDateTime,
    counter: HyperLogLog<131_072>, // 2^17
    salt: [u8; digest::SHA512_OUTPUT_LEN],
}

impl<F: Frequency> InnerUserCount<F> {
    fn new() -> Result<InnerUserCount<F>> {
        let frequency = F::default();
        let rng = rand::SystemRandom::new();
        let mut salt = [0u8; digest::SHA512_OUTPUT_LEN];
        rng.fill(&mut salt)
            .map_err(|_| anyhow!("failed to generate salt"))?;

        Ok(Self {
            frequency,
            next_reset: frequency.next_reset(),
            counter: HyperLogLog::default(),
            metric: Counter::default(),
            salt,
        })
    }

    fn inc<T: bincode::Encode>(&mut self, user_id: &T) -> Result<()> {
        // It is important that we do not store the user_id as it might be sensitive.
        // It is first hashed with a salt for good measure, and then we only use
        // the id for a probabilistic count using hyperloglog.
        self.maybe_reset();
        let bytes = bincode::encode_to_vec(user_id, common::bincode_config())?;
        let mut hash = [0u8; digest::SHA512_OUTPUT_LEN];

        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA512,
            100.try_into().unwrap(),
            &self.salt,
            &bytes,
            &mut hash,
        );

        let mut user_id = [0u8; 8];
        user_id.copy_from_slice(&hash[..8]);

        self.counter.add(u64::from_le_bytes(user_id));
        self.metric.store(self.counter.size() as u64);

        Ok(())
    }

    fn maybe_reset(&mut self) {
        if Utc::now().naive_utc() >= self.next_reset {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.counter.clear();
        self.next_reset = self.frequency.next_reset();
    }

    fn metric(&self) -> Counter {
        self.metric.clone()
    }
}

pub struct UserCount<F: Frequency> {
    inner: Arc<Mutex<InnerUserCount<F>>>,
}

impl<F: Frequency> UserCount<F> {
    pub fn new() -> Result<UserCount<F>> {
        Ok(Self {
            inner: Arc::new(Mutex::new(InnerUserCount::new()?)),
        })
    }

    pub fn inc<T: bincode::Encode>(&self, user_id: &T) -> Result<()> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .inc(user_id)
    }

    pub fn metric(&self) -> Counter {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .metric()
    }
}
