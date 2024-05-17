type Method = 'DELETE' | 'GET' | 'PUT' | 'POST' | 'HEAD' | 'TRACE' | 'PATCH';

let GLOBAL_API_BASE = '';
export const getApiBase = (options?: ApiOptions) => options?.apiBase ?? GLOBAL_API_BASE;
export const setGlobalApiBase = (apiBase: string) => (GLOBAL_API_BASE = apiBase);

export type ApiOptions = {
  fetch?: typeof fetch;
  apiBase?: string;
  headers?: Record<string, string>;
};

export const requestPlain = (
  method: Method,
  url: string,
  body?: unknown,
  options?: ApiOptions,
): {
  data: Promise<string>;
  cancel: (reason?: string) => void;
} => {
  let inFlight = true;
  const controller = new AbortController();
  const data = (options?.fetch ?? fetch)(`${getApiBase(options)}${url}`, {
    method: method.toUpperCase(),
    body: typeof body != 'undefined' ? JSON.stringify(body) : void 0,
    signal: controller.signal,
    headers: {
      ...options?.headers,
      ...(typeof body != 'undefined' ? { 'Content-Type': 'application/json' } : {}),
    },
  }).then(async (res) => {
    inFlight = false;
    if (res.ok) {
      const text = await res.text();
      try {
        return text;
      } catch (_) {
        throw text;
      }
    } else {
      throw res.text();
    }
  });

  return {
    data,
    cancel: (reason) => {
      if (inFlight) controller.abort(reason);
    },
  };
};

export const requestJson = <T>(
  method: Method,
  url: string,
  body?: unknown,
  options: ApiOptions = {},
): {
  data: Promise<T>;
  cancel: (reason?: string) => void;
} => {
  const { data, cancel } = requestPlain(method, url, body, options);
  return { data: data.then((text) => JSON.parse(text) as T), cancel };
};

export type SSEStream<T> = (
  event:
    | { type: 'message'; data: T }
    | {
        type: 'error';
        event: Event;
      },
) => void;

const sse = <T>(
  _method: Method,
  url: string,
  options?: ApiOptions,
): {
  cancel: () => void;
  listen: (stream: SSEStream<T>) => void;
} => {
  const source = new EventSource(`${getApiBase(options)}${url}`);

  let stream: SSEStream<T> | null = null;

  source.onmessage = (event) => {
    const data = event.data;
    stream?.({ type: 'message', data });
  };
  source.onerror = (event) => {
    stream?.({ type: 'error', event });
  };
  return {
    cancel: () => source.close(),
    listen: (newStream) => (stream = newStream),
  };
};

export const api = {
  autosuggest: (
    params: {
      q: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<Suggestion[]>(
      'POST',
      `/beta/api/autosuggest?${new URLSearchParams(params)}`,
      options,
    ),
  exploreExport: (body: ExploreExportOpticParams, options?: ApiOptions) =>
    requestPlain('POST', `/beta/api/explore/export`, body, options),
  hostsExport: (body: HostsExportOpticParams, options?: ApiOptions) =>
    requestPlain('POST', `/beta/api/hosts/export`, body, options),
  search: (body: ApiSearchQuery, options?: ApiOptions) =>
    requestJson<ApiSearchResult>('POST', `/beta/api/search`, body, options),
  searchSidebar: (body: SidebarQuery, options?: ApiOptions) =>
    requestJson<DisplayedSidebar>('POST', `/beta/api/search/sidebar`, body, options),
  searchSpellcheck: (body: SpellcheckQuery, options?: ApiOptions) =>
    requestJson<HighlightedSpellCorrection>('POST', `/beta/api/search/spellcheck`, body, options),
  searchWidget: (body: WidgetQuery, options?: ApiOptions) =>
    requestJson<Widget>('POST', `/beta/api/search/widget`, body, options),
  summarize: (
    query: {
      query: string;
      url: string;
    },
    options?: ApiOptions,
  ) => sse<string>('GET', `/beta/api/summarize?${new URLSearchParams(query)}`, options),
  webgraphHostIngoing: (
    query: {
      host: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<FullEdge[]>(
      'POST',
      `/beta/api/webgraph/host/ingoing?${new URLSearchParams(query)}`,
      options,
    ),
  webgraphHostKnows: (
    query: {
      host: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<KnowsHost>(
      'POST',
      `/beta/api/webgraph/host/knows?${new URLSearchParams(query)}`,
      options,
    ),
  webgraphHostOutgoing: (
    query: {
      host: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<FullEdge[]>(
      'POST',
      `/beta/api/webgraph/host/outgoing?${new URLSearchParams(query)}`,
      options,
    ),
  webgraphHostSimilar: (body: SimilarHostsParams, options?: ApiOptions) =>
    requestJson<ScoredHost[]>('POST', `/beta/api/webgraph/host/similar`, body, options),
  webgraphPageIngoing: (
    query: {
      page: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<FullEdge[]>(
      'POST',
      `/beta/api/webgraph/page/ingoing?${new URLSearchParams(query)}`,
      options,
    ),
  webgraphPageOutgoing: (
    query: {
      page: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<FullEdge[]>(
      'POST',
      `/beta/api/webgraph/page/outgoing?${new URLSearchParams(query)}`,
      options,
    ),
};

export type ApiSearchQuery = {
  countResultsExact?: boolean;
  flattenResponse?: boolean;
  hostRankings?: HostRankings;
  numResults?: number;
  optic?: string;
  page?: number;
  query: string;
  returnRankingSignals?: boolean;
  returnStructuredData?: boolean;
  safeSearch?: boolean;
  selectedRegion?: Region;
  signalCoefficients?: {};
};
export type ApiSearchResult =
  | (WebsitesResult & {
      _type: 'websites';
    })
  | (BangHit & {
      _type: 'bang';
    });
export type Bang = {
  c?: string;
  d?: string;
  r?: number;
  s?: string;
  sc?: string;
  t: string;
  u: string;
};
export type BangHit = {
  bang: Bang;
  redirectTo: UrlWrapper;
};
export type Calculation = {
  input: string;
  result: string;
};
export type CodeOrText =
  | {
      _type: 'code';
      value: string;
    }
  | {
      _type: 'text';
      value: string;
    };
export type Count =
  | {
      _type: 'exact';
      value: number;
    }
  | {
      _type: 'approximate';
      value: number;
    };
export type Definition = string;
export type DisplayedAnswer = {
  answer: string;
  prettyUrl: string;
  snippet: string;
  title: string;
  url: string;
};
export type DisplayedEntity = {
  imageId?: string;
  info: string & EntitySnippet[][];
  matchScore: number;
  relatedEntities: DisplayedEntity[];
  smallAbstract: EntitySnippet;
  title: string;
};
export type DisplayedSidebar =
  | {
      _type: 'entity';
      value: DisplayedEntity;
    }
  | {
      _type: 'stackOverflow';
      value: {
        answer: StackOverflowAnswer;
        title: string;
      };
    };
export type DisplayedWebpage = {
  domain: string;
  likelyHasAds: boolean;
  likelyHasPaywall: boolean;
  prettyUrl: string;
  rankingSignals?: {};
  richSnippet?: RichSnippet;
  score?: number;
  site: string;
  snippet: Snippet;
  structuredData?: StructuredData[];
  title: string;
  url: string;
};
export type EntitySnippet = {
  fragments: EntitySnippetFragment[];
};
export type EntitySnippetFragment =
  | {
      kind: 'normal';
      text: string;
    }
  | {
      href: string;
      kind: 'link';
      text: string;
    };
export type Example = string;
export type ExploreExportOpticParams = {
  chosenHosts: string[];
  similarHosts: string[];
};
export type FullEdge = {
  from: Node;
  label: string;
  to: Node;
};
export type HighlightedFragment = {
  kind: HighlightedKind;
  text: string;
};
export type HighlightedKind = 'normal' | 'highlighted';
export const HIGHLIGHTED_KINDS = ['normal', 'highlighted'] satisfies HighlightedKind[];
export type HighlightedSpellCorrection = {
  highlighted: HighlightedFragment[];
  raw: string;
};
export type HostRankings = {
  blocked: string[];
  disliked: string[];
  liked: string[];
};
export type HostsExportOpticParams = {
  hostRankings: HostRankings;
};
export type KnowsHost =
  | {
      _type: 'known';
      host: string;
    }
  | {
      _type: 'unknown';
    };
export type Lemma = string;
export type Node = {
  name: string;
};
export type OneOrManyProperty = Property | Property[];
export type OneOrManyString = string | string[];
export type PartOfSpeech = 'noun' | 'verb' | 'adjective' | 'adjectiveSatellite' | 'adverb';
export const PART_OF_SPEECHES = [
  'noun',
  'verb',
  'adjective',
  'adjectiveSatellite',
  'adverb',
] satisfies PartOfSpeech[];
export type PartOfSpeechMeaning = {
  meanings: WordMeaning[];
  pos: PartOfSpeech;
};
export type Property = string | StructuredData;
export type Region = 'All' | 'Denmark' | 'France' | 'Germany' | 'Spain' | 'US';
export const REGIONS = ['All', 'Denmark', 'France', 'Germany', 'Spain', 'US'] satisfies Region[];
export type ReturnBody =
  | {
      _type: 'all';
    }
  | {
      _type: 'truncated';
      value: number;
    };
export type RichSnippet = {
  _type: 'stackOverflowQA';
  answers: StackOverflowAnswer[];
  question: StackOverflowQuestion;
};
export type ScoredHost = {
  description?: string;
  host: string;
  score: number;
};
export type SidebarQuery = {
  query: string;
};
export type SignalEnumDiscriminants =
  | 'bm25_title'
  | 'bm25_title_bigrams'
  | 'bm25_title_trigrams'
  | 'bm25_clean_body'
  | 'bm25_clean_body_bigrams'
  | 'bm25_clean_body_trigrams'
  | 'bm25_stemmed_title'
  | 'bm25_stemmed_clean_body'
  | 'bm25_all_body'
  | 'bm25_keywords'
  | 'bm25_backlink_text'
  | 'idf_sum_url'
  | 'idf_sum_site'
  | 'idf_sum_domain'
  | 'idf_sum_site_no_tokenizer'
  | 'idf_sum_domain_no_tokenizer'
  | 'idf_sum_domain_name_no_tokenizer'
  | 'idf_sum_domain_if_homepage'
  | 'idf_sum_domain_name_if_homepage_no_tokenizer'
  | 'idf_sum_domain_if_homepage_no_tokenizer'
  | 'idf_sum_title_if_homepage'
  | 'cross_encoder_snippet'
  | 'cross_encoder_title'
  | 'host_centrality'
  | 'host_centrality_rank'
  | 'page_centrality'
  | 'page_centrality_rank'
  | 'is_homepage'
  | 'fetch_time_ms'
  | 'update_timestamp'
  | 'tracker_score'
  | 'region'
  | 'query_centrality'
  | 'inbound_similarity'
  | 'lambda_mart'
  | 'url_digits'
  | 'url_slashes'
  | 'link_density'
  | 'title_embedding_similarity'
  | 'keyword_embedding_similarity';
export const SIGNAL_ENUM_DISCRIMINANTS = [
  'bm25_title',
  'bm25_title_bigrams',
  'bm25_title_trigrams',
  'bm25_clean_body',
  'bm25_clean_body_bigrams',
  'bm25_clean_body_trigrams',
  'bm25_stemmed_title',
  'bm25_stemmed_clean_body',
  'bm25_all_body',
  'bm25_keywords',
  'bm25_backlink_text',
  'idf_sum_url',
  'idf_sum_site',
  'idf_sum_domain',
  'idf_sum_site_no_tokenizer',
  'idf_sum_domain_no_tokenizer',
  'idf_sum_domain_name_no_tokenizer',
  'idf_sum_domain_if_homepage',
  'idf_sum_domain_name_if_homepage_no_tokenizer',
  'idf_sum_domain_if_homepage_no_tokenizer',
  'idf_sum_title_if_homepage',
  'cross_encoder_snippet',
  'cross_encoder_title',
  'host_centrality',
  'host_centrality_rank',
  'page_centrality',
  'page_centrality_rank',
  'is_homepage',
  'fetch_time_ms',
  'update_timestamp',
  'tracker_score',
  'region',
  'query_centrality',
  'inbound_similarity',
  'lambda_mart',
  'url_digits',
  'url_slashes',
  'link_density',
  'title_embedding_similarity',
  'keyword_embedding_similarity',
] satisfies SignalEnumDiscriminants[];
export type SignalScore = {
  coefficient: number;
  value: number;
};
export type SimilarHostsParams = {
  hosts: string[];
  topN: number;
};
export type Snippet = {
  date?: string;
  text: TextSnippet;
};
export type SpellcheckQuery = {
  query: string;
};
export type StackOverflowAnswer = {
  accepted: boolean;
  body: CodeOrText[];
  date: string;
  upvotes: number;
  url: string;
};
export type StackOverflowQuestion = {
  body: CodeOrText[];
};
export type StructuredData = {
  _type?: OneOrManyString;
};
export type Suggestion = {
  highlighted: HighlightedFragment[];
  raw: string;
};
export type TextSnippet = {
  fragments: HighlightedFragment[];
};
export type ThesaurusWidget = {
  meanings: PartOfSpeechMeaning[];
  term: Lemma;
};
export type UrlWrapper = string;
export type WebsitesResult = {
  hasMoreResults: boolean;
  numHits: Count;
  searchDurationMs: number;
  webpages: DisplayedWebpage[];
};
export type Widget =
  | {
      _type: 'calculator';
      value: Calculation;
    }
  | {
      _type: 'thesaurus';
      value: ThesaurusWidget;
    };
export type WidgetQuery = {
  query: string;
};
export type WordMeaning = {
  definition: Definition;
  examples: Example[];
  similar: Lemma[];
};
