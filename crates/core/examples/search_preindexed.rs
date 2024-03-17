use optics::HostRankings;
use rand::seq::SliceRandom;
use stract::{
    bangs::Bangs,
    config::{
        ApiConfig, ApiThresholds, CollectorConfig, CorrectionConfig, LLMConfig, SnippetConfig,
        WidgetsConfig,
    },
    distributed::member::ShardId,
    image_store::Image,
    index::Index,
    inverted_index::RetrievedWebpage,
    ranking::{inbound_similarity::InboundSimilarity, pipeline::PrecisionRankingWebpage},
    searcher::{api::ApiSearcher, live::LiveSearcher, LocalSearcher, SearchQuery},
    Result,
};
struct Searcher(LocalSearcher<Index>);

impl stract::searcher::distributed::SearchClient for Searcher {
    async fn search_initial(
        &self,
        query: &SearchQuery,
    ) -> Vec<stract::searcher::InitialSearchResultShard> {
        let res = self.0.search_initial(query, true).unwrap();

        vec![stract::searcher::InitialSearchResultShard {
            local_result: res,
            shard: ShardId::new(0),
        }]
    }

    async fn retrieve_webpages(
        &self,
        top_websites: &[(usize, stract::searcher::ScoredWebpagePointer)],
        query: &str,
    ) -> Vec<(usize, stract::ranking::pipeline::PrecisionRankingWebpage)> {
        let pointers = top_websites
            .iter()
            .map(|(_, p)| p.website.pointer.clone())
            .collect::<Vec<_>>();

        let res = self
            .0
            .retrieve_websites(&pointers, query)
            .unwrap()
            .into_iter()
            .zip(top_websites.iter().map(|(i, p)| (*i, p.website.clone())))
            .map(|(ret, (i, ran))| (i, PrecisionRankingWebpage::new(ret, ran)))
            .collect::<Vec<_>>();

        res
    }

    async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        Ok(self.0.get_webpage(url))
    }

    async fn get_homepage_descriptions(
        &self,
        urls: &[url::Url],
    ) -> std::collections::HashMap<url::Url, String> {
        let mut res = std::collections::HashMap::new();

        for url in urls {
            if let Some(homepage) = self.0.get_homepage(url) {
                if let Some(desc) = homepage.description() {
                    res.insert(url.clone(), desc.clone());
                }
            }
        }

        res
    }

    async fn get_entity_image(
        &self,
        _image_id: &str,
        _max_height: Option<u64>,
        _max_width: Option<u64>,
    ) -> Result<Option<Image>> {
        Ok(None)
    }

    async fn search_entity(&self, _query: &str) -> Option<stract::entity_index::EntityMatch> {
        None
    }
}

#[tokio::main]
pub async fn main() {
    let index = Index::open("data/index").unwrap();

    let collector_conf = CollectorConfig {
        ..Default::default()
    };

    let config = ApiConfig {
        queries_csv_path: "data/queries_us.csv".to_string(),
        host: "0.0.0.0:8000".parse().unwrap(),
        prometheus_host: "0.0.0.0:8001".parse().unwrap(),
        crossencoder_model_path: None,
        lambda_model_path: None,
        dual_encoder_model_path: None,
        spell_checker_path: Some("data/web_spell".to_string()),
        bangs_path: "data/bangs.json".to_string(),
        summarizer_path: "data/summarizer".to_string(),
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
        correction_config: CorrectionConfig::default(),
        llm: LLMConfig {
            api_base: "http://localhost:4000/v1".to_string(),
            model: "data/mistral-7b-instruct-v0.2.Q4_K_M.gguf".to_string(),
            api_key: None,
        },
    };

    let mut queries = stract::autosuggest::Autosuggest::load_csv(&config.queries_csv_path)
        .unwrap()
        .all()
        .unwrap();

    queries.shuffle(&mut rand::thread_rng());

    let mut searcher = LocalSearcher::new(index);
    searcher.set_inbound_similarity(
        InboundSimilarity::open("data/centrality/inbound_similarity").unwrap(),
    );
    searcher.set_collector_config(collector_conf);
    searcher.set_snippet_config(SnippetConfig {
        num_words_for_lang_detection: Some(250),
        max_considered_words: Some(10_000),
        ..Default::default()
    });
    let bangs = Bangs::from_path(&config.bangs_path);

    let searcher = Searcher(searcher);

    let searcher: ApiSearcher<Searcher, LiveSearcher> =
        ApiSearcher::new(searcher, None, None, None, None, bangs, config);

    for query in &queries {
        let mut desc = "search '".to_string();
        desc.push_str(query);
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
