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

use crate::Result;
use anyhow::anyhow;
use eventsource_stream::Eventsource;
use futures::stream::Stream;
use tokio_stream::StreamExt;

pub struct OpenAiApi {
    api: String,
    key: Option<String>,
    top_p: f64,
    temp: f64,
    model: String,
    max_tokens: Option<u64>,
    stop: Vec<String>,
}

impl OpenAiApi {
    pub fn builder(api: String, model: String) -> OpenAiApiBuilder {
        OpenAiApiBuilder::new(api, model)
    }

    fn payload(&self, prompt: &str) -> Result<serde_json::Value> {
        let mut payload = serde_json::json!({
            "prompt": prompt,
            "temperature": self.temp,
            "top_p": self.top_p,
            "stop": self.stop.clone(),
            "model": self.model.clone(),
        });

        if let Some(max_tokens) = self.max_tokens {
            payload["max_tokens"] = serde_json::json!(max_tokens);
        }

        Ok(payload)
    }

    pub async fn generate(&self, prompt: &str) -> Result<String> {
        let client = reqwest::Client::new();
        let mut req = client.post(format!("{}/completions", &self.api));

        if let Some(key) = &self.key {
            req = req.bearer_auth(key);
        }

        let res = req
            .json(&self.payload(prompt)?)
            .send()
            .await?
            .text()
            .await?;

        let res: serde_json::Value = serde_json::from_str(&res)?;

        let res = res
            .get("choices")
            .ok_or(anyhow!("unexepected response format"))?
            .get(0)
            .ok_or(anyhow!("unexpected response format"))?
            .get("text")
            .ok_or(anyhow!("unexpected response format"))?
            .as_str()
            .ok_or(anyhow!("unexpected response format"))?;

        Ok(String::from(res))
    }

    pub async fn stream(&self, prompt: &str) -> Result<impl Stream<Item = Result<String>>> {
        let client = reqwest::Client::new();

        let mut payload = self.payload(prompt)?;
        payload["stream"] = serde_json::json!(true);

        let mut req = client.post(format!("{}/completions", &self.api));

        if let Some(key) = &self.key {
            req = req.bearer_auth(key);
        }

        Ok(req
            .json(&payload)
            .send()
            .await?
            .bytes_stream()
            .eventsource()
            .map(|event| {
                let event = event?;
                let data: serde_json::Value = serde_json::from_str(&event.data)?;

                let text = data
                    .get("choices")
                    .ok_or(anyhow!("unexpected response format"))?
                    .get(0)
                    .ok_or(anyhow!("unexpected response format"))?
                    .get("text")
                    .ok_or(anyhow!("unexpected response format"))?
                    .as_str()
                    .ok_or(anyhow!("unexpected response format"))?;

                Ok(text.to_string())
            }))
    }
}

pub struct OpenAiApiBuilder {
    api: String,
    key: Option<String>,
    model: String,
    top_p: f64,
    temp: f64,
    max_tokens: Option<u64>,
    stop: Vec<String>,
}

impl OpenAiApiBuilder {
    pub fn new(api: String, model: String) -> Self {
        let mut api = api;

        if let Some(p) = api.strip_suffix('/') {
            api = p.to_string();
        }

        Self {
            api,
            model,
            top_p: 0.9,
            temp: 1.0,
            max_tokens: None,
            stop: vec![],
            key: None,
        }
    }

    pub fn api_key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    pub fn top_p(mut self, top_p: f64) -> Self {
        self.top_p = top_p;
        self
    }

    pub fn temp(mut self, temp: f64) -> Self {
        self.temp = temp;
        self
    }

    pub fn max_tokens(mut self, max_tokens: u64) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    pub fn stop(mut self, stop: Vec<&str>) -> Self {
        self.stop = stop.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn build(self) -> OpenAiApi {
        OpenAiApi {
            api: self.api,
            top_p: self.top_p,
            temp: self.temp,
            key: self.key,
            max_tokens: self.max_tokens,
            stop: self.stop,
            model: self.model,
        }
    }
}
