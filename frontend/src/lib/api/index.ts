// deno-lint-ignore-file ban-types

import { match } from 'ts-pattern';
import type { components } from './schema.d.ts';
import { send, sse, type Props, API_BASE, type ApiOptions } from './com';

export const api = {
  search: (props: Props<'/beta/api/search', 'post'>, options?: ApiOptions) => {
    const { data, cancel } = send('/beta/api/search', 'post', 'json', props, options);

    return { data: data.then((res) => res[200]), cancel };
  },

  autosuggest: (props: Props<'/beta/api/autosuggest', 'post'>, options?: ApiOptions) => {
    const { data, cancel } = send('/beta/api/autosuggest', 'post', 'parameters', props, options);
    return { data: data.then((res) => res[200]), cancel };
  },

  similarSites: (
    props: Props<'/beta/api/webgraph/similar_sites', 'post'>,
    options?: ApiOptions,
  ) => {
    const { data, cancel } = send(
      '/beta/api/webgraph/similar_sites',
      'post',
      'json',
      props,
      options,
    );
    return { data: data.then((res) => res[200]), cancel };
  },

  knowsSite: (props: Props<'/beta/api/webgraph/knows_site', 'post'>, options?: ApiOptions) => {
    const { data, cancel } = send(
      '/beta/api/webgraph/knows_site',
      'post',
      'parameters',
      props,
      options,
    );
    return { data: data.then((res) => res[200]), cancel };
  },

  factCheck: (props: Props<'/beta/api/fact_check', 'post'>, options?: ApiOptions) => {
    const { data, cancel } = send('/beta/api/fact_check', 'post', 'json', props, options);
    return { data: data.then((res) => res[200]), cancel };
  },

  alice: (
    props: Props<'/beta/api/alice', 'get'>,
    stream: (
      msg:
        | { type: 'begin' }
        | { type: 'content'; data: ExecutionState }
        | {
            type: 'done';
          },
    ) => void,
    options?: ApiOptions,
  ) => {
    const { cancel, listen } = sse('/beta/api/alice', 'parameters', props, options);
    stream({ type: 'begin' });
    listen((e) => {
      match(e)
        .with({ type: 'message' }, ({ data }) => stream({ type: 'content', data }))
        .with({ type: 'error' }, () => {
          stream({ type: 'done' });
          cancel();
        })
        .exhaustive();
    });
  },

  summarize: (
    props: Props<'/beta/api/summarize', 'get'>,
    stream: (
      msg:
        | { type: 'begin' }
        | { type: 'content'; data: string }
        | {
            type: 'done';
          },
    ) => void,
    options?: ApiOptions,
  ) => {
    const { cancel, listen } = sse('/beta/api/summarize', 'parameters', props, options);
    stream({ type: 'begin' });
    listen((e) => {
      match(e)
        .with({ type: 'message' }, ({ data }) => stream({ type: 'content', data }))
        .with({ type: 'error' }, () => {
          stream({ type: 'done' });
          cancel();
        })
        .exhaustive();
    });
  },

  sitesExportOptic: (props: Props<'/beta/api/sites/export', 'post'>, options?: ApiOptions) => {
    const { data, cancel } = send('/beta/api/sites/export', 'post', 'json', props, options);
    return { data: data.then((res) => res[200]), cancel };
  },

  exploreExportOptic: (props: Props<'/beta/api/explore/export', 'post'>, options?: ApiOptions) => {
    const { data, cancel } = send('/beta/api/explore/export', 'post', 'json', props, options);
    return { data: data.then((res) => res[200]), cancel };
  },

  queryId: ({ query, urls }: { query: string; urls: string[] }, options?: ApiOptions) => {
    const { signal, abort } = new AbortController();
    let finished = false;
    const data = (options?.fetch ?? fetch)(`${API_BASE}/improvement/store`, {
      method: 'POST',
      signal,
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: query,
        urls: urls,
      }),
    })
      .then((response) => response.text())
      .then((data) => {
        finished = true;
        return data;
      });

    return {
      data,
      abort: () => {
        if (!finished) {
          abort();
        }
      },
    };
  },

  sendImprovementClick: ({ queryId, clickIndex }: { queryId: string; clickIndex: number }) => {
    window.navigator.sendBeacon(`${API_BASE}/improvement/click?qid=${queryId}&click=${clickIndex}`);
  },
};

export type SearchResults = components['schemas']['ApiSearchResult'];
export type WebsitesResult = components['schemas']['WebsitesResult'];
export type Webpage = components['schemas']['DisplayedWebpage'];
export type TextSnippet = components['schemas']['TextSnippet'];
export type Sidebar = components['schemas']['Sidebar'];
export type Entity = components['schemas']['DisplayedEntity'];
export type Widget = components['schemas']['Widget'];
export type DisplayedAnswer = components['schemas']['DisplayedAnswer'];
export type ScoredSite = components['schemas']['ScoredSite'];
export type Suggestion = components['schemas']['Suggestion'];
export type Region = components['schemas']['Region'];

export type StackOverflowAnswer = components['schemas']['StackOverflowAnswer'];
export type StackOverflowQuestion = components['schemas']['StackOverflowQuestion'];

export type FactCheckResponse = components['schemas']['FactCheckResponse'];
export type ExecutionState = components['schemas']['ExecutionState'];
