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
  search: (body: ApiSearchQuery, options?: ApiOptions) =>
    requestJson<ApiSearchResult>('POST', `/beta/api/search`, body, options),
  sitesExport: (body: SitesExportOpticParams, options?: ApiOptions) =>
    requestPlain('POST', `/beta/api/sites/export`, body, options),
  summarize: (
    query: {
      query: string;
      url: string;
    },
    options?: ApiOptions,
  ) => sse<string>('GET', `/beta/api/summarize?${new URLSearchParams(query)}`, options),
  webgraphHostIngoing: (
    query: {
      site: string;
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
      site: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<KnowsSite>(
      'POST',
      `/beta/api/webgraph/host/knows?${new URLSearchParams(query)}`,
      options,
    ),
  webgraphHostOutgoing: (
    query: {
      site: string;
    },
    options?: ApiOptions,
  ) =>
    requestJson<FullEdge[]>(
      'POST',
      `/beta/api/webgraph/host/outgoing?${new URLSearchParams(query)}`,
      options,
    ),
  webgraphHostSimilar: (body: SimilarSitesParams, options?: ApiOptions) =>
    requestJson<ScoredSite[]>('POST', `/beta/api/webgraph/host/similar`, body, options),
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
  countResults?: boolean;
  fetchDiscussions?: boolean;
  flattenResponse?: boolean;
  numResults?: number;
  optic?: string;
  page?: number;
  query: string;
  returnRankingSignals?: boolean;
  safeSearch?: boolean;
  selectedRegion?: Region;
  siteRankings?: SiteRankings;
};
export type ApiSearchResult =
  | (WebsitesResult & {
      type: 'websites';
    })
  | (BangHit & {
      type: 'bang';
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
      type: 'code';
      value: string;
    }
  | {
      type: 'text';
      value: string;
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
      type: 'entity';
      value: DisplayedEntity;
    }
  | {
      type: 'stackOverflow';
      value: {
        answer: StackOverflowAnswer;
        title: string;
      };
    };
export type DisplayedWebpage = {
  domain: string;
  prettyUrl: string;
  rankingSignals?: {};
  site: string;
  snippet: Snippet;
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
  chosenSites: string[];
  similarSites: string[];
};
export type FullEdge = {
  from: Node;
  label: string;
  to: Node;
};
export type HighlightedSpellCorrection = {
  highlighted: string;
  raw: string;
};
export type KnowsSite =
  | {
      site: string;
      type: 'known';
    }
  | {
      type: 'unknown';
    };
export type Lemma = string;
export type Node = {
  name: string;
};
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
export type Region = 'All' | 'Denmark' | 'France' | 'Germany' | 'Spain' | 'US';
export const REGIONS = ['All', 'Denmark', 'France', 'Germany', 'Spain', 'US'] satisfies Region[];
export type ScoredSite = {
  description?: string;
  score: number;
  site: string;
};
export type SignalScore = {
  coefficient: number;
  value: number;
};
export type SimilarSitesParams = {
  sites: string[];
  topN: number;
};
export type SiteRankings = {
  blocked: string[];
  disliked: string[];
  liked: string[];
};
export type SitesExportOpticParams = {
  siteRankings: SiteRankings;
};
export type Snippet =
  | {
      date?: string;
      text: TextSnippet;
      type: 'normal';
    }
  | {
      answers: StackOverflowAnswer[];
      question: StackOverflowQuestion;
      type: 'stackOverflowQA';
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
export type Suggestion = {
  highlighted: string;
  raw: string;
};
export type TextSnippet = {
  fragments: TextSnippetFragment[];
};
export type TextSnippetFragment = {
  kind: TextSnippetFragmentKind;
  text: string;
};
export type TextSnippetFragmentKind = 'normal' | 'highlighted';
export const TEXT_SNIPPET_FRAGMENT_KINDS = [
  'normal',
  'highlighted',
] satisfies TextSnippetFragmentKind[];
export type ThesaurusWidget = {
  meanings: PartOfSpeechMeaning[];
  term: Lemma;
};
export type UrlWrapper = string;
export type WebsitesResult = {
  directAnswer?: DisplayedAnswer;
  discussions?: DisplayedWebpage[];
  hasMoreResults: boolean;
  numHits?: number;
  searchDurationMs: number;
  sidebar?: DisplayedSidebar;
  spellCorrectedQuery?: HighlightedSpellCorrection;
  webpages: DisplayedWebpage[];
  widget?: Widget;
};
export type Widget =
  | {
      type: 'calculator';
      value: Calculation;
    }
  | {
      type: 'thesaurus';
      value: ThesaurusWidget;
    };
export type WordMeaning = {
  definition: Definition;
  examples: Example[];
  similar: Lemma[];
};
