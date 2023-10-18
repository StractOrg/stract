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

use std::{rc::Rc, sync::Arc};

use aes_gcm::{Aes256Gcm, Key};
use leaky_queue::LeakyQueue;
use tch::{IndexOp, Kind, Tensor};

use crate::{
    alice::ExecutionState,
    llm_utils::{self, ClonableTensor},
};

use super::{
    raw_model::RawModel, AnyTransitionValidator, EncodedEncryptedState, EncryptedState, Error,
    ModelState, Result, Searcher, Tokenizer, TransitionValidator,
};
use crate::alice::{ModelWebsite, SimplifiedWebsite};

// const TAU: f64 = 0.3;
const TEMP: f64 = 0.4; // 0.4
const TOP_P: f64 = 0.7; //0.5

const MAX_NUM_QUERIES: usize = 3;

pub enum TokenGeneratorState {
    InProgress,
    Finished,
}

pub struct AliceTokenGenerator {
    model: Rc<RawModel>,
    // state needs to be a queue of size 2 to have the ability
    // to generate a new token if the current one is banned
    states: LeakyQueue<ModelState>,
    // the state of the model after each search result has been
    // loaded. This allows us to go back to the end of the previous search
    // and force speak if the model wants us to search for the same query again.
    end_search_states: Vec<LeakyQueue<ModelState>>,
    end_tokens: Vec<i64>,
    generation_state: TokenGeneratorState,
    max_new_tokens: Option<usize>,
    num_generated_tokens: usize,
    banned_tokens: Vec<i64>,
}

impl AliceTokenGenerator {
    pub fn new(
        model: Rc<RawModel>,
        end_tokens: Vec<i64>,
        state: Tensor,
        tokens: &[i64],
        max_new_tokens: Option<usize>,
    ) -> Result<Self> {
        if tokens.is_empty() {
            return Err(Error::EmptyInput.into());
        }

        // this must be 2, otherwise `.front()` and `.back()` calls are wrong throughout the generation code
        let num_states = 2;

        let mut states = LeakyQueue::new(num_states);

        let state = ClonableTensor(state);

        for _ in 0..num_states {
            states.push(ModelState {
                state: state.clone(),
                next_token: -1,
            });
        }

        let mut generator = Self {
            model,
            states,
            end_tokens,
            generation_state: TokenGeneratorState::InProgress,
            max_new_tokens,
            num_generated_tokens: 0,
            end_search_states: Vec::new(),
            banned_tokens: Vec::new(),
        };
        generator.load_tokens(tokens);

        Ok(generator)
    }

    pub fn load_tokens(&mut self, tokens: &[i64]) {
        for token in tokens {
            let (logits, new_state) = self.model.forward(
                *token,
                Some(
                    &self
                        .states
                        .back()
                        .expect("there should be 2 states at all times")
                        .state
                        .0,
                ),
            );
            let probs = logits
                .softmax(-1, Kind::Float)
                .squeeze()
                .to(tch::Device::Cpu);

            let next_token = llm_utils::sample_nucleus(probs, TEMP, TOP_P);
            self.states.push(ModelState {
                state: ClonableTensor(new_state),
                next_token,
            });

            if let Some(state) = self.states.front_mut() {
                state.next_token = *token;
            }
        }
    }

    fn load_search_result(&mut self, tokens: &[i64]) {
        self.load_tokens(tokens);
        self.end_search_states.push(self.states.clone());
    }

    fn go_to_last_search(&mut self) {
        if let Some(last_search) = self.end_search_states.pop() {
            self.states = last_search;
        }
    }

    pub fn set_end_tokens(&mut self, tokens: &[i64]) {
        if self.end_tokens != tokens {
            self.end_tokens = tokens.to_vec();
        }
    }

    pub fn set_max_new_tokens(&mut self, max_new_tokens: Option<usize>) {
        if self.max_new_tokens != max_new_tokens {
            self.num_generated_tokens = 0;
            self.max_new_tokens = max_new_tokens;
        }
    }

    pub fn reset_tokens_counter(&mut self) {
        self.num_generated_tokens = 0;
    }

    pub fn set_banned_tokens(&mut self, tokens: &[i64]) {
        if self.banned_tokens != tokens {
            self.banned_tokens = tokens.to_vec();
        }
    }

    fn previous_state(&mut self) {
        if let Some(f) = self.states.pop() {
            self.states.push(f.clone());
            self.states.push(f);
        }
    }
}

impl Iterator for AliceTokenGenerator {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self.generation_state {
            TokenGeneratorState::InProgress => {
                let s = self
                    .states
                    .back()
                    .expect("there should be 2 states at all times");
                let mut token = s.next_token;
                let mut state = s.state.clone();

                if self.end_tokens.contains(&token) {
                    self.generation_state = TokenGeneratorState::Finished;
                    return None;
                }

                if let Some(max_new_tokens) = self.max_new_tokens {
                    if self.num_generated_tokens >= max_new_tokens {
                        if let Some(tok) = self.end_tokens.first() {
                            token = *tok;
                        }

                        self.generation_state = TokenGeneratorState::Finished;
                    }
                }

                if self.banned_tokens.contains(&token) {
                    let (output, new_state) = {
                        let s = self
                            .states
                            .front()
                            .expect("there should be 2 states at all times");
                        self.model.forward(s.next_token, Some(&s.state.0))
                    };

                    let output = output.squeeze();

                    let probs = output
                        .softmax(-1, Kind::Float)
                        .squeeze()
                        .to(tch::Device::Cpu);

                    for banned_token in &self.banned_tokens {
                        probs
                            .i(*banned_token)
                            .copy_(&Tensor::from_slice(&[0.0]).squeeze());
                    }

                    let next_token = llm_utils::sample_nucleus(probs, TEMP, TOP_P);
                    token = next_token;
                    state = ClonableTensor(new_state);
                }

                let (output, state) = self.model.forward(token, Some(&state.0));
                let output = output.squeeze();

                let probs = output
                    .softmax(-1, Kind::Float)
                    .squeeze()
                    .to(tch::Device::Cpu);

                for banned_token in &self.banned_tokens {
                    probs
                        .i(*banned_token)
                        .copy_(&Tensor::from_slice(&[0.0]).squeeze());
                }

                let next_token = llm_utils::sample_nucleus(probs, TEMP, TOP_P);
                self.states.push(ModelState {
                    state: ClonableTensor(state),
                    next_token,
                });

                self.num_generated_tokens += 1;

                Some(token)
            }
            TokenGeneratorState::Finished => None,
        }
    }
}

pub struct AliceStringGenerator {
    token_generator: AliceTokenGenerator,
    tokenizer: Arc<Tokenizer>,
    tokens: Vec<i64>,
}

impl Iterator for AliceStringGenerator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.token_generator.next() {
                Some(token) => {
                    self.tokens.push(token);
                    if let Some(s) = self.tokenizer.decode(&self.tokens).ok().and_then(|s| {
                        if !s.contains('\u{fffd}') {
                            // valid utf-8 string
                            Some(s)
                        } else {
                            None
                        }
                    }) {
                        self.tokens.clear();
                        return Some(s);
                    }
                }
                None if !self.tokens.is_empty() => {
                    let s = self.tokenizer.decode(&self.tokens).ok().and_then(|s| {
                        if !s.contains('\u{fffd}') {
                            Some(s)
                        } else {
                            None
                        }
                    });

                    self.tokens.clear();

                    return s;
                }
                None => return None,
            }
        }
    }
}

#[derive(Debug)]
pub enum RawAction {
    Search { query: Vec<i64> },
    Speak { token: i64 },
}

enum State {
    Thought {
        search: TransitionValidator,
        speaking: TransitionValidator,
    },
    QueryBuild {
        tokens: Vec<i64>,
        end: AnyTransitionValidator,
    },
    Speaking {
        banned_tokens: TTLBannedTokens,
        single_end: AnyTransitionValidator,
        double_newline: TransitionValidator,
        max_new_tokens: Option<usize>,
    },
}

impl State {
    fn new_thought(tokenizer: &Tokenizer) -> Self {
        let search = TransitionValidator::new(tokenizer.encode("Search:".to_string()).unwrap());
        let speaking = TransitionValidator::new(tokenizer.encode("Alice:".to_string()).unwrap());

        Self::Thought { search, speaking }
    }

    fn new_query_build(tokenizer: &Tokenizer) -> Self {
        let new_line = TransitionValidator::new(tokenizer.encode("\n".to_string()).unwrap());
        let end_of_text =
            TransitionValidator::new(tokenizer.encode("<|endoftext|>".to_string()).unwrap());
        let end = AnyTransitionValidator::new(vec![new_line, end_of_text]);

        Self::QueryBuild {
            tokens: Vec::new(),
            end,
        }
    }

    fn new_speaking(initially_banned_tokens: Vec<i64>) -> Self {
        let end = TransitionValidator::new(vec![0]);
        let double_newline_single_tok = TransitionValidator::new(vec![535]);
        let single_end = AnyTransitionValidator::new(vec![end, double_newline_single_tok]);

        let double_newline = TransitionValidator::new(vec![187, 187]);

        Self::Speaking {
            single_end,
            double_newline,
            max_new_tokens: Some(1024),
            banned_tokens: TTLBannedTokens::new(initially_banned_tokens, 2), // the tokens should only be banned at the start of generation
        }
    }
}

struct TTLBannedTokens {
    tokens: Vec<i64>,
    orig_ttl: usize,
    ttl: usize,
}

impl TTLBannedTokens {
    fn new(tokens: Vec<i64>, ttl: usize) -> Self {
        Self {
            tokens,
            ttl,
            orig_ttl: ttl,
        }
    }

    fn reset_ttl(&mut self) {
        self.ttl = self.orig_ttl;
    }

    fn tick(&mut self) {
        if self.ttl > 0 {
            self.ttl -= 1;
        }
    }

    fn set_banned_tokens(&self, token_generator: &mut AliceTokenGenerator) {
        if self.ttl > 0 {
            token_generator.set_banned_tokens(&self.tokens);
        } else {
            token_generator.set_banned_tokens(&[]);
        }
    }
}

pub struct RawActionGenerator {
    state: State,
    tokenizer: Arc<Tokenizer>,
    token_generator: AliceTokenGenerator,
    queries_performed: usize,
    max_num_queries: usize,
}

impl RawActionGenerator {
    pub fn new(token_generator: AliceTokenGenerator, tokenizer: Arc<Tokenizer>) -> Self {
        let state = State::new_thought(&tokenizer);

        let mut token_generator = token_generator;
        token_generator.set_end_tokens(&[]);

        Self {
            state,
            tokenizer,
            token_generator,
            queries_performed: 0,
            max_num_queries: MAX_NUM_QUERIES,
        }
    }

    fn force_speaking(&mut self) {
        self.token_generator.load_tokens(
            &self
                .tokenizer
                .encode("Alice[thought]: I now know the final answer.\n\n".to_string())
                .unwrap(),
        );

        self.state = State::new_speaking(vec![
            self.tokenizer.encode(" [thought]:".to_string()).unwrap()[0],
            self.tokenizer.encode("[thought]:".to_string()).unwrap()[0],
        ]);

        self.token_generator
            .load_tokens(&self.tokenizer.encode("Alice:".to_string()).unwrap());
    }
}

impl Iterator for RawActionGenerator {
    type Item = RawAction;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                State::Thought { search, speaking } => {
                    let token = self.token_generator.next()?;
                    self.token_generator.set_max_new_tokens(Some(1024));

                    if self.queries_performed >= self.max_num_queries {
                        self.token_generator.reset_tokens_counter();
                        self.force_speaking();
                    } else if search.validate(token) {
                        self.token_generator.reset_tokens_counter();
                        self.state = State::new_query_build(&self.tokenizer);
                    } else if speaking.validate(token) {
                        self.token_generator.reset_tokens_counter();
                        self.state = State::new_speaking(vec![]);
                    }
                }
                State::QueryBuild { tokens, end } => {
                    let token = self.token_generator.next()?;
                    self.token_generator.set_max_new_tokens(Some(1024));
                    tokens.push(token);

                    if end.validate(token) {
                        let query = tokens.clone();
                        *tokens = Vec::new();
                        self.token_generator.reset_tokens_counter();
                        self.state = State::new_thought(&self.tokenizer);
                        self.queries_performed += 1;
                        return Some(RawAction::Search { query });
                    }
                }
                State::Speaking {
                    single_end,
                    double_newline,
                    max_new_tokens,
                    banned_tokens,
                } => {
                    self.token_generator.set_max_new_tokens(*max_new_tokens);
                    banned_tokens.set_banned_tokens(&mut self.token_generator);

                    let token = self.token_generator.next()?;

                    if double_newline.validate(token) {
                        banned_tokens.reset_ttl();
                        self.token_generator.reset_tokens_counter();
                        self.state = State::new_thought(&self.tokenizer);
                        return None;
                    } else if single_end.validate(token) {
                        banned_tokens.reset_ttl();
                        self.token_generator.reset_tokens_counter();
                        self.token_generator.previous_state();
                        self.token_generator.load_tokens(&[187, 187]);
                        self.state = State::new_thought(&self.tokenizer);
                        return None;
                    } else {
                        banned_tokens.tick();
                        return Some(RawAction::Speak { token });
                    }
                }
            }
        }
    }
}

pub enum Action {
    Search { query: String },
    Speak { text: String },
}

pub struct ActionGenerator {
    raw: RawActionGenerator,
    tokens_to_speak: Vec<i64>,
}

impl ActionGenerator {
    pub fn new(raw_action_gen: RawActionGenerator) -> ActionGenerator {
        Self {
            raw: raw_action_gen,
            tokens_to_speak: Vec::new(),
        }
    }
}

impl Iterator for ActionGenerator {
    type Item = Action;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let action = self.raw.next();

            match action {
                Some(RawAction::Search { query }) => {
                    return Some(Action::Search {
                        query: self
                            .raw
                            .tokenizer
                            .decode(&query)
                            .unwrap_or_default()
                            .trim()
                            .to_ascii_lowercase(),
                    });
                }
                Some(RawAction::Speak { token }) => {
                    self.tokens_to_speak.push(token);

                    if let Some(s) = self
                        .raw
                        .tokenizer
                        .decode(&self.tokens_to_speak)
                        .ok()
                        .and_then(|s| {
                            if !s.contains('\u{fffd}') {
                                // valid utf-8 string
                                Some(s)
                            } else {
                                None
                            }
                        })
                    {
                        self.tokens_to_speak.clear();
                        return Some(Action::Speak { text: s });
                    }
                }
                None if !self.tokens_to_speak.is_empty() => {
                    let s = self
                        .raw
                        .tokenizer
                        .decode(&self.tokens_to_speak)
                        .ok()
                        .and_then(|s| {
                            if !s.contains('\u{fffd}') {
                                Some(s)
                            } else {
                                None
                            }
                        });

                    self.tokens_to_speak.clear();

                    return s.map(|s| Action::Speak { text: s });
                }
                None => {
                    return None;
                }
            }
        }
    }
}

pub struct ActionExecutor {
    generator: ActionGenerator,
    searcher: Searcher,
    query_to_search: Option<String>,
    queries_performed: Vec<String>,
    has_finished: bool,
    encryption_key: Key<Aes256Gcm>,
}

unsafe impl Send for ActionExecutor {}

impl ActionExecutor {
    pub fn new(
        action_gen: ActionGenerator,
        searcher: Searcher,
        encryption_key: Key<Aes256Gcm>,
    ) -> Self {
        ActionExecutor {
            generator: action_gen,
            searcher,
            query_to_search: None,
            queries_performed: Vec::new(),
            has_finished: false,
            encryption_key,
        }
    }
    pub fn state(&self) -> Tensor {
        self.generator
            .raw
            .token_generator
            .states
            .back()
            .expect("there should be 2 states at all times")
            .state
            .clone()
            .0
    }

    fn load_search_result(&mut self, result: &[SimplifiedWebsite]) {
        let result = result
            .iter()
            .map(|r| ModelWebsite::from(r.clone()))
            .collect::<Vec<_>>();

        let json = serde_json::to_string(&result).unwrap();

        let tokens = self
            .generator
            .raw
            .tokenizer
            .encode(format!("Result: {json}\n\n"))
            .unwrap();
        self.generator
            .raw
            .token_generator
            .load_search_result(&tokens);

        let tokens = self
            .generator
            .raw
            .tokenizer
            .encode("Alice[thought]:".to_string())
            .unwrap();
        self.generator.raw.token_generator.load_tokens(&tokens);
    }
}

impl Iterator for ActionExecutor {
    type Item = ExecutionState;

    fn next(&mut self) -> Option<Self::Item> {
        if self.has_finished {
            return None;
        }

        if let Some(query) = self.query_to_search.take() {
            if self.queries_performed.contains(&query) {
                self.generator.raw.token_generator.go_to_last_search();
                self.generator.raw.force_speaking();
            } else {
                let res = self.searcher.search(&query).unwrap_or_default();

                tracing::debug!("loading search results");
                self.load_search_result(&res);
                tracing::debug!("done loading search results");
                self.queries_performed.push(query.clone());

                return Some(ExecutionState::SearchResult { query, result: res });
            }
        }

        let action = self.generator.next();

        match action {
            Some(Action::Search { query }) => {
                self.query_to_search = Some(query.clone());
                Some(ExecutionState::BeginSearch { query })
            }
            Some(Action::Speak { text }) => Some(ExecutionState::Speaking { text }),
            None if !self.has_finished => {
                self.has_finished = true;

                let state: Vec<Vec<f32>> =
                    self.state().to_device(tch::Device::Cpu).try_into().unwrap();

                let encrypted = EncryptedState::encrypt(state, &self.encryption_key);

                Some(ExecutionState::Done {
                    state: EncodedEncryptedState::encode(encrypted),
                })
            }
            None => None,
        }
    }
}
