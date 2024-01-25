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

use std::{
    collections::HashMap,
    fmt::Display,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Group already exists")]
    GroupExists,
}

#[derive(Default, Clone)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    pub fn store(&self, val: u64) {
        self.0.store(val, Ordering::SeqCst);
    }
}

pub enum PrometheusMetric {
    Counter(Counter),
}

impl PrometheusMetric {
    fn prom_type(&self) -> &'static str {
        match self {
            PrometheusMetric::Counter(_) => "counter",
        }
    }

    fn prom_val(&self) -> String {
        match self {
            PrometheusMetric::Counter(counter) => format!("{}", counter.0.load(Ordering::SeqCst)),
        }
    }
}

impl From<Counter> for PrometheusMetric {
    fn from(counter: Counter) -> Self {
        Self::Counter(counter)
    }
}

type Name = String;

#[derive(Default)]
pub struct PrometheusRegistry {
    groups: HashMap<Name, PrometheusGroup>,
}

impl PrometheusRegistry {
    pub fn new_group(
        &mut self,
        name: Name,
        help: Option<String>,
    ) -> Result<&mut PrometheusGroup, Error> {
        if self.groups.contains_key(&name) {
            return Err(Error::GroupExists);
        }

        self.groups.insert(
            name.clone(),
            PrometheusGroup {
                metrics: Vec::new(),
                help,
                forced_timestamp: None,
                name: name.clone(),
            },
        );

        Ok(self.groups.get_mut(&name).unwrap())
    }
}

impl Display for PrometheusRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, (_, r)) in self.groups.iter().enumerate() {
            f.write_str(&format!("{r}"))?;
            f.write_str("\n")?;

            if i < self.groups.len() - 1 {
                f.write_str("\n")?;
            }
        }

        Ok(())
    }
}

pub struct Label {
    pub key: String,
    pub val: String,
}

struct LabelledMetric {
    metric: PrometheusMetric,
    labels: Vec<Label>,
}
impl Display for LabelledMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.labels.is_empty() {
            f.write_str("{")?;

            let num_labels = self.labels.len();
            for (i, label) in self.labels.iter().enumerate() {
                f.write_str(&format!("{}=\"{}\"", label.key, label.val))?;

                if i < num_labels - 1 {
                    f.write_str(",")?;
                }
            }

            f.write_str("}")?;
        }

        f.write_str(" ")?;
        f.write_str(&self.metric.prom_val())?;

        Ok(())
    }
}

pub struct PrometheusGroup {
    metrics: Vec<LabelledMetric>,
    help: Option<String>,
    forced_timestamp: Option<u128>,
    name: Name,
}
impl PrometheusGroup {
    pub fn register<M: Into<PrometheusMetric>>(&mut self, metric: M, labels: Vec<Label>) {
        self.metrics.push(LabelledMetric {
            metric: metric.into(),
            labels,
        });
    }
}

impl Display for PrometheusGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(help) = self.help.as_ref() {
            f.write_str("# HELP ")?;
            f.write_str(&self.name)?;
            f.write_str(" ")?;
            f.write_str(help)?;
            f.write_str("\n")?;
        }

        let timestamp = match self.forced_timestamp {
            Some(timestamp) => timestamp,
            None => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        };

        if let Some(first) = self.metrics.first() {
            f.write_str("# TYPE ")?;
            f.write_str(&self.name)?;
            f.write_str(" ")?;
            f.write_str(first.metric.prom_type())?;
        }

        for m in &self.metrics {
            f.write_str("\n")?;
            f.write_str(&self.name)?;
            f.write_str(&format!("{m}"))?;
            f.write_str(" ")?;
            f.write_str(&format!("{timestamp}"))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn counter() {
        let counter = Counter::default();
        let mut registry = PrometheusRegistry::default();

        let group = registry
            .new_group(
                "test_counter".to_string(),
                Some("Test counter help.".to_string()),
            )
            .unwrap();
        group.register(
            counter.clone(),
            vec![Label {
                key: "test_label".to_string(),
                val: "123".to_string(),
            }],
        );

        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        for group in registry.groups.values_mut() {
            group.forced_timestamp = Some(t);
        }

        let expected = format!(
            r##"# HELP test_counter Test counter help.
# TYPE test_counter counter
test_counter{{test_label="123"}} 0 {t}
"##
        );
        assert_eq!(format!("{registry}"), expected);

        counter.inc();

        let expected = format!(
            r##"# HELP test_counter Test counter help.
# TYPE test_counter counter
test_counter{{test_label="123"}} 1 {t}
"##
        );
        assert_eq!(format!("{registry}"), expected);
    }
}
