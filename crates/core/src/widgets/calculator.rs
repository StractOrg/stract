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

use crate::widgets::Error;
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    sync::{atomic::AtomicUsize, Arc, Mutex},
};
use utoipa::ToSchema;

static DICE_REGEX: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"^d[0-9]+").unwrap());

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Calculation {
    pub input: String,
    pub result: String,
}

async fn get_rates() -> Result<CurrencyExchange> {
    let client = reqwest::Client::new();
    let xml = client
        .get("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml")
        .send()
        .await?
        .text()
        .await?;

    let mut rates = HashMap::new();
    let mut buf = Vec::new();
    let mut reader = quick_xml::Reader::from_str(&xml);

    // read all `Cube` nodes that has the `currency` attribute
    // and insert them into the `rates` map
    while let Ok(event) = reader.read_event_into(&mut buf) {
        match event {
            quick_xml::events::Event::Empty(ref e) if e.name().as_ref() == b"Cube" => {
                if let Some(currency) = e
                    .attributes()
                    .find(|a| a.as_ref().unwrap().key.as_ref() == b"currency")
                    .map(|a| a.unwrap().value.to_vec())
                {
                    let rate = e
                        .attributes()
                        .find(|a| a.as_ref().unwrap().key.as_ref() == b"rate")
                        .map(|a| a.unwrap().value.to_vec())
                        .unwrap();

                    rates.insert(
                        String::from_utf8(currency)?,
                        String::from_utf8(rate)?.parse::<f64>()?,
                    );
                }
            }
            quick_xml::events::Event::Eof => break,
            _ => (),
        }
    }

    rates.insert("EUR".to_string(), 1.0);

    Ok(CurrencyExchange { rates })
}

#[derive(Debug, Default)]
struct CurrencyExchange {
    rates: HashMap<String, f64>,
}

impl CurrencyExchange {
    fn get_rate(
        &self,
        currency: &str,
    ) -> Result<
        f64,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        match self.rates.get(currency) {
            Some(rate) => Ok(*rate),
            None => Err(anyhow!("No exchange rate for currency: {}", currency).into()),
        }
    }
}

pub enum ExchangeUpdate {
    #[allow(dead_code)] // used in tests
    None,
    AsyncTokio,
}

struct MaxIterations {
    max_iterations: usize,
    iterations: AtomicUsize,
}
impl MaxIterations {
    fn new(max_iterations: usize) -> Self {
        Self {
            max_iterations,
            iterations: AtomicUsize::new(0),
        }
    }
}

impl fend_core::Interrupt for MaxIterations {
    fn should_interrupt(&self) -> bool {
        self.iterations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            > self.max_iterations
    }
}

pub struct Calculator {
    exchange: Arc<Mutex<Arc<CurrencyExchange>>>,
}

impl Calculator {
    pub fn new(exchange_update: ExchangeUpdate) -> Self {
        let exchange = Arc::new(Mutex::new(Arc::new(Default::default())));

        match exchange_update {
            ExchangeUpdate::None => {}
            ExchangeUpdate::AsyncTokio => {
                let exchange_clone = exchange.clone();
                tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_secs(60 * 60 * 24));

                    loop {
                        if let Ok(rates) = get_rates().await {
                            *exchange_clone.lock().unwrap() = Arc::new(rates);
                        }

                        interval.tick().await;
                    }
                });
            }
        }

        Self { exchange }
    }

    pub fn try_calculate(&self, expr: &str) -> Result<Calculation, Error> {
        let expr = expr.replace(['"', '\''], "");
        // check if expr has at least one digit
        if !expr.chars().any(|c| c.is_ascii_digit()) {
            return Err(Error::CalculatorParse);
        }

        // if expr starts with "d[0-9]+", wrap it in "roll(...)"
        let expr = if DICE_REGEX.is_match(&expr) {
            format!("roll({})", expr)
        } else {
            expr.to_string()
        };

        let mut context = fend_core::Context::new();

        let exchange: Arc<CurrencyExchange> = self.exchange.lock().unwrap().clone();

        context.set_exchange_rate_handler_v1(move |currency: &str| exchange.get_rate(currency));

        context.set_random_u32_fn(|| {
            let mut rng = rand::thread_rng();
            rng.gen()
        });

        let interrupt = MaxIterations::new(256);

        let res = fend_core::evaluate_with_interrupt(&expr, &mut context, &interrupt)
            .map_err(|_| Error::CalculatorParse)?;

        if res.get_main_result() == expr {
            return Err(Error::CalculatorParse);
        }

        let mut result: String = String::new();

        for span in res.get_main_result_spans() {
            match span.kind() {
                fend_core::SpanKind::Number => {
                    let num = span.string().parse::<f64>()?;

                    if num.fract() > 0.01 {
                        result.push_str(format!("{:.2}", num).as_str());
                    } else {
                        result.push_str(num.to_string().as_str());
                    }
                }
                _ => result.push_str(span.string()),
            }
        }

        Ok(Calculation {
            input: expr.to_string(),
            result,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_calculates_simple_expressions() {
        let calc = Calculator::new(ExchangeUpdate::None);
        assert_eq!(calc.try_calculate("2+2").unwrap().result, 4.0.to_string());
        assert_eq!(calc.try_calculate("2*2").unwrap().result, 4.0.to_string());
        assert_eq!(calc.try_calculate("2*3").unwrap().result, 6.0.to_string());
        assert_eq!(calc.try_calculate("6/2").unwrap().result, 3.0.to_string());
    }

    #[test]
    fn it_respects_paranthesis() {
        let calc = Calculator::new(ExchangeUpdate::None);

        assert_eq!(
            calc.try_calculate("2+2*6").unwrap().result,
            14.0.to_string()
        );
        assert_eq!(
            calc.try_calculate("(2+2)*6").unwrap().result,
            24.0.to_string()
        );
    }
}
