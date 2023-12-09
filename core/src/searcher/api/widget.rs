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

use std::ops::Range;
use std::sync::Arc;

use itertools::Itertools;
use optics::Optic;

#[cfg(feature = "libtorch")]
use crate::qa_model::QaModel;
use crate::ranking::models::lambdamart::LambdaMART;
use crate::ranking::pipeline::{AsRankingWebsite, RankingPipeline};
use crate::search_prettifier::DisplayedAnswer;
use crate::search_prettifier::DisplayedWebpage;
use crate::searcher::api::{add_ranking_signals, combine_results, ScoredWebsitePointer};
use crate::searcher::{DistributedSearcher, SearchQuery};
use crate::widgets::Widget;
use crate::{ceil_char_boundary, floor_char_boundary, query, Result};
use crate::{
    config::{ApiThresholds, CollectorConfig},
    widgets::Widgets,
};

#[cfg(feature = "libtorch")]
pub struct WidgetManager {
    widgets: Widgets,
    distributed_searcher: Arc<DistributedSearcher>,
    thresholds: ApiThresholds,
    collector_config: CollectorConfig,
    lambda_model: Option<Arc<LambdaMART>>,
    qa_model: Option<Arc<QaModel>>,
}

#[cfg(not(feature = "libtorch"))]
pub struct WidgetManager {
    widgets: Widgets,
    distributed_searcher: Arc<DistributedSearcher>,
    thresholds: ApiThresholds,
    collector_config: CollectorConfig,
    lambda_model: Option<Arc<LambdaMART>>,
}

impl WidgetManager {
    #[cfg(feature = "libtorch")]
    pub fn new(
        widgets: Widgets,
        distributed_searcher: Arc<DistributedSearcher>,
        thresholds: ApiThresholds,
        collector_config: CollectorConfig,
        lambda_model: Option<Arc<LambdaMART>>,
        qa_model: Option<Arc<QaModel>>,
    ) -> WidgetManager {
        Self {
            widgets,
            distributed_searcher,
            thresholds,
            collector_config,
            lambda_model,
            qa_model,
        }
    }

    #[cfg(not(feature = "libtorch"))]
    pub fn new(
        widgets: Widgets,
        distributed_searcher: Arc<DistributedSearcher>,
        thresholds: ApiThresholds,
        collector_config: CollectorConfig,
        lambda_model: Option<Arc<LambdaMART>>,
    ) -> WidgetManager {
        Self {
            widgets,
            distributed_searcher,
            thresholds,
            collector_config,
            lambda_model,
        }
    }

    pub async fn discussions(&self, query: &SearchQuery) -> Result<Option<Vec<DisplayedWebpage>>> {
        if !query.fetch_discussions || query.optic.is_some() || query.page > 0 {
            return Ok(None);
        }

        const NUM_RESULTS: usize = 10;

        let mut query = SearchQuery {
            query: query.query.clone(),
            num_results: NUM_RESULTS,
            optic: Some(Optic::parse(include_str!("discussions.optic")).unwrap()),
            host_rankings: query.host_rankings.clone(),
            return_ranking_signals: query.return_ranking_signals,
            ..Default::default()
        };

        // This pipeline should be created before the first search is performed
        // so the query knows how many results to fetch from the indices
        let pipeline: RankingPipeline<ScoredWebsitePointer> = RankingPipeline::first_stage(
            &mut query,
            self.lambda_model.clone(),
            self.collector_config.clone(),
            NUM_RESULTS,
        );

        let initial_results = self.distributed_searcher.search_initial(&query).await;

        if initial_results.is_empty() {
            return Ok(None);
        }

        let num_results: usize = initial_results
            .iter()
            .map(|res| res.local_result.websites.len())
            .sum();

        if num_results < NUM_RESULTS / 2 {
            return Ok(None);
        }

        let (top_websites, _) = combine_results(
            self.collector_config.clone(),
            initial_results,
            vec![],
            pipeline,
        );

        let scores: Vec<_> = top_websites
            .iter()
            .map(|pointer| pointer.as_ranking().score)
            .collect();

        let median = if scores.len() % 2 == 0 {
            (scores[(scores.len() / 2) - 1] + scores[scores.len() / 2]) / 2.0
        } else {
            scores[scores.len() / 2]
        };

        if median < self.thresholds.discussions_widget {
            return Ok(None);
        }

        let mut result: Vec<DisplayedWebpage> = self
            .distributed_searcher
            .retrieve_webpages(
                &top_websites
                    .clone()
                    .into_iter()
                    .filter_map(|pointer| {
                        if let ScoredWebsitePointer::Normal(p) = pointer {
                            Some(p)
                        } else {
                            None
                        }
                    })
                    .enumerate()
                    .collect_vec(),
                &query.query,
            )
            .await
            .into_iter()
            .map(|(_, webpage)| webpage.into_retrieved_webpage())
            .map(DisplayedWebpage::from)
            .collect();

        if query.return_ranking_signals {
            add_ranking_signals(&mut result, &top_websites);
        }

        Ok(Some(result))
    }

    pub async fn widget(&self, query: &SearchQuery) -> Option<Widget> {
        if query.page > 0 {
            return None;
        }

        let parsed_terms = query::parser::parse(&query.query);

        self.widgets.widget(
            parsed_terms
                .into_iter()
                .filter_map(|term| {
                    if let query::parser::Term::Simple(simple) = *term {
                        Some(String::from(simple))
                    } else {
                        None
                    }
                })
                .join(" ")
                .as_str(),
        )
    }

    #[cfg(feature = "libtorch")]
    pub fn answer(
        &self,
        query: &str,
        webpages: &mut Vec<DisplayedWebpage>,
    ) -> Option<DisplayedAnswer> {
        self.qa_model.as_ref().and_then(|qa_model| {
            let contexts: Vec<_> = webpages
                .iter()
                .take(1)
                .filter_map(|webpage| webpage.snippet.text())
                .map(|t| t.fragments.iter().map(|f| f.text()).join(""))
                .collect();

            match qa_model.run(query, &contexts) {
                Some(answer) => {
                    let answer_webpage = webpages.remove(answer.context_idx);
                    let snip = answer_webpage
                        .snippet
                        .text()
                        .unwrap()
                        .fragments
                        .iter()
                        .map(|f| f.text())
                        .join("");
                    Some(DisplayedAnswer {
                        title: answer_webpage.title,
                        url: answer_webpage.url,
                        pretty_url: answer_webpage.pretty_url,
                        snippet: generate_answer_snippet(&snip, answer.offset.clone()),
                        answer: snip[answer.offset].to_string(),
                    })
                }
                None => None,
            }
        })
    }

    #[cfg(not(feature = "libtorch"))]
    pub fn answer(
        &self,
        query: &str,
        webpages: &mut Vec<DisplayedWebpage>,
    ) -> Option<DisplayedAnswer> {
        None
    }
}

fn generate_answer_snippet(body: &str, answer_offset: Range<usize>) -> String {
    let mut best_start = 0;
    let mut best_end = 0;
    const SNIPPET_LENGTH: usize = 200;

    if body.is_empty() || answer_offset.start > body.len() - 1 {
        return body.to_string();
    }

    for (idx, _) in body.char_indices().filter(|(_, c)| *c == '.') {
        if idx > answer_offset.end + SNIPPET_LENGTH {
            break;
        }

        if idx < answer_offset.start {
            best_start = idx;
        }

        if idx > answer_offset.end {
            best_end = idx;
        }
    }

    if (answer_offset.end - best_start > SNIPPET_LENGTH) || (best_start >= best_end) {
        if answer_offset.end - answer_offset.start >= SNIPPET_LENGTH {
            let end = floor_char_boundary(body, answer_offset.start + SNIPPET_LENGTH);

            return "<b>".to_string() + &body[answer_offset.start..end] + "</b>";
        }

        let chars_either_side = (SNIPPET_LENGTH - (answer_offset.end - answer_offset.start)) / 2;

        let start = ceil_char_boundary(
            body,
            answer_offset
                .start
                .checked_sub(chars_either_side)
                .unwrap_or_default(),
        );
        let mut end = ceil_char_boundary(body, answer_offset.end + chars_either_side);

        if end >= body.len() {
            end = floor_char_boundary(body, body.len());
        }

        body[start..answer_offset.start].to_string()
            + "<b>"
            + &body[answer_offset.clone()]
            + "</b>"
            + &body[answer_offset.end..end]
    } else {
        let mut res = body[best_start..answer_offset.start].to_string()
            + "<b>"
            + &body[answer_offset.clone()]
            + "</b>";

        let remaining_chars = SNIPPET_LENGTH - (res.len() - 7);
        let end = ceil_char_boundary(body, (remaining_chars + answer_offset.end).min(best_end));

        res += &body[answer_offset.end..end];

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_generate_answer_snippet() {
        assert_eq!(
            generate_answer_snippet("this is a test", 0..4),
            "<b>this</b> is a test".to_string()
        );

        assert_eq!(
            generate_answer_snippet("this is a test", 0..1000),
            "<b>this is a test</b>".to_string()
        );
        assert_eq!(
            generate_answer_snippet("this is a test", 1000..2000),
            "this is a test".to_string()
        );
        let input = r#"
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test this is a long test
            "#;

        let res = generate_answer_snippet(input, 0..500);
        assert!(!res.is_empty());
        assert!(res.len() > 100);
        assert!(res.len() < input.len());
        assert!(res.starts_with("<b>"));
        assert!(res.ends_with("</b>"));

        assert_eq!(generate_answer_snippet("", 0..2000), "".to_string());
    }
}
