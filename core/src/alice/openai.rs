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

use std::path::Path;
use std::sync::Arc;

use reqwest_eventsource::EventSource;
use tokio_stream::StreamExt;

use crate::summarizer::ExtractiveSummarizer;

use super::Error;
use super::ExecutionState;
use super::ModelWebsite;
use super::Result;
use super::Searcher;
use super::SimplifiedWebsite;

const MAX_NUM_SEARCHES: usize = 3;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(tag = "@type", rename_all = "camelCase")]
pub enum Message {
    User {
        message: String,
    },
    Alice {
        message: String,
        queries: Vec<String>,
    },
}

fn prompt(user_question: &str, conv: &[Message]) -> String {
    let mut prompt = r#"System: Your name is Alice, and you are an AI assistant developed by Stract. Below is a conversation between you and a user. Help the user as best you can.
You can lookup information on the web in the following format:
Alice[thought]: I should look up "<keywords>" on the web.
Search i: <query>
Result: <search result>

This Thought/Search/Result can repeat N times.
You should always cite the source of your statements using the format [search i source j] at the end of the statement.

The search results from your previous messages have been truncated.

When you are ready to answer the user, use the following format:
Alice[thought]: I now know enough to answer the user.
Alice: <answer>

You should only use the information you find on the web. If you couldn't find a good answer to the user, you should tell them and ask for clarification.

Begin!"#.to_string();

    //  truncate conv
    let conv = if conv.len() > 4 {
        &conv[conv.len() - 4..]
    } else {
        conv
    };

    for msg in conv {
        match msg {
            Message::User { message } => {
                prompt.push_str("\n\n");
                prompt.push_str(format!("User: {}", message).as_str());
            }
            Message::Alice { message, queries } => {
                prompt.push('\n');
                for query in queries {
                    prompt.push('\n');
                    prompt.push_str(&format!(
                        "Alice[thought]: I should look up \"{query}\" on the web."
                    ));
                    prompt.push('\n');
                    prompt.push_str(&format!("Search i: {query}\n"));
                    prompt.push_str("Result: <truncated>");
                }
                prompt.push('\n');
                prompt.push_str("Alice[thought]: I now know enough to answer the user.");
                prompt.push('\n');
                prompt.push_str(&format!("Alice: {}", message));
            }
        }
    }

    prompt.push_str("\n\n");
    prompt.push_str(format!("User: {}", user_question).as_str());

    prompt
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OpenaiParams {
    model: String,
    prompt: String,
    max_tokens: usize,
    temperature: f32,
    top_p: f32,
    stream: bool,
    stop: Vec<String>,
}

async fn thoughts(prompt: &str, api_key: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let res: serde_json::Value = client
        .post("https://api.openai.com/v1/completions")
        .bearer_auth(api_key)
        .json(&OpenaiParams {
            model: "text-davinci-003".to_string(),
            prompt: prompt.to_string(),
            max_tokens: 100,
            temperature: 0.7,
            top_p: 0.7,
            stream: false,
            stop: vec!["Result:".to_string(), "Alice:".to_string()],
        })
        .send()
        .await?
        .json()
        .await?;

    let text = res["choices"][0]["text"].as_str().unwrap().to_string();

    Ok(dbg!(text))
}

async fn speak(prompt: &str, api_key: &str) -> Result<EventSource> {
    let client = reqwest::Client::new();
    let req = client
        .post("https://api.openai.com/v1/completions")
        .bearer_auth(api_key)
        .json(&OpenaiParams {
            model: "text-davinci-003".to_string(),
            prompt: prompt.to_string(),
            max_tokens: 512,
            temperature: 0.7,
            top_p: 0.7,
            stream: true,
            stop: vec!["<|endoftext|>".to_string()],
        });

    let res = EventSource::new(req)?;

    Ok(res)
}

#[derive(Debug)]
enum State {
    BeginSearch { query: String },
    PerformSearch { query: String },

    Thinking,
    Speaking,
}

fn next_state(completion: &str) -> Result<State> {
    let completion = completion.trim();

    if completion.ends_with("Alice:")
        || completion.contains("I now know enough to answer the user.")
    {
        Ok(State::Speaking)
    } else if completion.contains("Search") {
        // find last "Search \d: <query>" and extract query
        let mut query = None;
        for line in completion.lines().rev() {
            if line.starts_with("Search") {
                query = Some(line.split(':').nth(1).unwrap().trim());
                break;
            }
        }

        if let Some(query) = query {
            Ok(State::BeginSearch {
                query: query.to_string(),
            })
        } else {
            Err(Error::UnexpectedCompletion)
        }
    } else {
        Err(Error::UnexpectedCompletion)
    }
}

pub struct Alice {
    api_key: String,
    summarizer: Arc<ExtractiveSummarizer>,
}

impl Alice {
    pub fn open<P: AsRef<Path>>(summarizer_path: P, api_key: &str) -> Result<Self> {
        let mut summarizer = ExtractiveSummarizer::open(summarizer_path, 1)?;
        summarizer.set_window_size(30);

        let summarizer = Arc::new(summarizer);

        Ok(Self {
            api_key: api_key.to_string(),
            summarizer,
        })
    }

    pub fn new_executor(
        &self,
        conversation: Vec<Message>,
        optic_url: Option<String>,
        search_url: String,
    ) -> Result<Executor> {
        let user_question = match conversation.last() {
            Some(Message::User { message }) => message.clone(),
            _ => return Err(Error::LastMessageNotUser),
        };

        let prev_conv = conversation[..conversation.len() - 1].to_vec();

        let searcher = Searcher {
            url: search_url,
            optic_url,
            summarizer: self.summarizer.clone(),
        };

        Ok(Executor::new(
            prev_conv,
            self.api_key.clone(),
            user_question,
            searcher,
        ))
    }
}

#[derive(Clone)]
struct Search {
    query: String,
    results: Vec<SimplifiedWebsite>,
}

pub struct Executor {
    api_key: String,
    prev_conv: Vec<Message>,
    current_searches: Vec<Search>,
    user_question: String,
    state: State,
    searcher: Searcher,
    speaking_stream: Option<EventSource>,
    performed_searches: usize,
}

impl Executor {
    fn new(
        prev_conv: Vec<Message>,
        api_key: String,
        user_question: String,
        searcher: Searcher,
    ) -> Self {
        Self {
            api_key,
            state: State::Thinking,
            prev_conv,
            user_question,
            current_searches: Vec::new(),
            speaking_stream: None,
            searcher,
            performed_searches: 0,
        }
    }

    fn build_prompt(&self, speaking: bool) -> String {
        let mut prompt = prompt(&self.user_question, &self.prev_conv);

        for (i, search) in self.current_searches.clone().into_iter().enumerate() {
            prompt.push_str("\n\n");
            prompt.push_str(
                format!(
                    "Alice[thought]: I should look up \"{}\" on the web.",
                    search.query
                )
                .as_str(),
            );
            prompt.push_str(format!("\nSearch {}: {}", i + 1, search.query).as_str());
            let res = search
                .results
                .into_iter()
                .take(3)
                .map(ModelWebsite::from)
                .collect::<Vec<_>>();

            prompt
                .push_str(format!("\nResponse: {}", serde_json::to_string(&res).unwrap()).as_str());
        }

        prompt.push_str("\n\n");

        if speaking {
            prompt.push_str("Alice[thought]: I now know enough to answer the user.");
            prompt.push_str("\n\n");
            prompt.push_str("Alice:");
        } else {
            prompt.push_str("Alice[thought]:");
        }

        dbg!(prompt)
    }

    pub async fn next(&mut self) -> Result<Option<ExecutionState>> {
        loop {
            match &mut self.state {
                State::BeginSearch { query } => {
                    let query = query.clone();

                    if self.current_searches.iter().any(|s| s.query == query) {
                        self.state = State::Speaking;
                    } else {
                        self.state = State::PerformSearch {
                            query: query.clone(),
                        };

                        return Ok(Some(ExecutionState::BeginSearch { query }));
                    }
                }
                State::PerformSearch { query } => {
                    let query = query.to_string();
                    let res = self.searcher.search_async(&query).await?;

                    self.current_searches.push(Search {
                        query: query.clone(),
                        results: res.clone(),
                    });

                    self.performed_searches += 1;

                    if self.performed_searches >= MAX_NUM_SEARCHES {
                        self.state = State::Speaking;
                    } else {
                        self.state = State::Thinking;
                    }

                    return Ok(Some(ExecutionState::SearchResult { query, result: res }));
                }
                State::Thinking => {
                    let prompt = self.build_prompt(false);
                    let cur_thoughts = thoughts(&prompt, &self.api_key).await?;
                    let next_state = dbg!(next_state(&cur_thoughts))?;
                    self.state = next_state;
                }
                State::Speaking => {
                    if self.speaking_stream.is_none() {
                        let prompt = self.build_prompt(true);
                        self.speaking_stream = Some(speak(&prompt, &self.api_key).await?);
                    }

                    let stream = self.speaking_stream.as_mut().unwrap();
                    match stream.next().await {
                        Some(event) => {
                            let event = event?;

                            match event {
                                reqwest_eventsource::Event::Open => {}
                                reqwest_eventsource::Event::Message(msg) => {
                                    let res: serde_json::Value = serde_json::from_str(&msg.data)?;
                                    let text =
                                        res["choices"][0]["text"].as_str().unwrap().to_string();

                                    return Ok(Some(ExecutionState::Speaking { text }));
                                }
                            }
                        }
                        None => return Ok(None),
                    }
                }
            }
        }
    }
}
