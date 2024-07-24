use optics::HostRankings;
use rand::seq::SliceRandom;
use stract::{
    bangs::Bangs,
    config::{
        defaults, ApiConfig, ApiThresholds, CollectorConfig, CorrectionConfig, SnippetConfig,
        WidgetsConfig,
    },
    index::Index,
    searcher::{
        api::ApiSearcher, live::LiveSearcher, LocalSearchClient, LocalSearcher, SearchQuery,
    },
    webgraph::Webgraph,
};

#[tokio::main]
pub async fn main() {
    let index = Index::open("data/index").unwrap();

    let collector_conf = CollectorConfig {
        ..Default::default()
    };

    let config = ApiConfig {
        host: "0.0.0.0:8000".parse().unwrap(),
        prometheus_host: "0.0.0.0:8001".parse().unwrap(),
        crossencoder_model_path: None,
        lambda_model_path: None,
        dual_encoder_model_path: None,
        bangs_path: Some("data/bangs.json".to_string()),
        query_store_db_host: None,
        cluster_id: "api".to_string(),
        gossip_seed_nodes: None,
        gossip_addr: "0.0.0.0:8002".parse().unwrap(),
        collector: collector_conf.clone(),
        thresholds: ApiThresholds::default(),
        widgets: WidgetsConfig {
            thesaurus_paths: vec!["data/english-wordnet-2022-subset.ttl".to_string()],
            calculator_fetch_currencies_exchange: false,
        },
        spell_check: Some(stract::config::ApiSpellCheck {
            path: "data/web_spell".to_string(),
            correction_config: CorrectionConfig::default(),
        }),
        max_concurrent_searches: defaults::Api::max_concurrent_searches(),
        max_similar_hosts: defaults::Api::max_similar_hosts(),
        top_phrases_for_autosuggest: defaults::Api::top_phrases_for_autosuggest(),
    };

    let mut searcher = LocalSearcher::new(index);

    let mut queries: Vec<_> = searcher
        .top_key_phrases(1_000_000)
        .into_iter()
        .map(|phrase| phrase.text().to_string())
        .collect();
    queries.shuffle(&mut rand::thread_rng());

    searcher.set_collector_config(collector_conf);
    searcher.set_snippet_config(SnippetConfig {
        num_words_for_lang_detection: Some(250),
        max_considered_words: Some(10_000),
        ..Default::default()
    });
    let bangs = Bangs::from_path(config.bangs_path.as_ref().unwrap());

    let searcher = stract::searcher::LocalSearchClient::from(searcher);

    let searcher: ApiSearcher<LocalSearchClient, LiveSearcher, Webgraph> =
        ApiSearcher::new(searcher, bangs, config);

    for query in queries {
        let mut query = query;
        query.push(' ');
        let query = query.repeat(32);

        let mut desc = "search '".to_string();
        desc.push_str(&query);
        desc.push('\'');

        println!("{desc}");

        searcher
            .search(&SearchQuery {
                query: query.to_string(),
                host_rankings: Some(HostRankings {
                    liked: vec![],
                    disliked: vec![],
                    blocked: vec![],
                }),
                ..Default::default()
            })
            .await
            .unwrap();
    }
}
