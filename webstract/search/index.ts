// deno-lint-ignore-file ban-types

import { match } from "ts-pattern";
import type { components, paths } from "./schema.d.ts";

type Values<T> = T[keyof T];

type Props<P extends keyof paths, M extends keyof paths[P]> =
  paths[P][M] extends
    { requestBody: { content: { "application/json": infer B extends {} } } } ? B
    : paths[P][M] extends { parameters: { path: infer B extends {} } } ? B
    : paths[P][M] extends { parameters: { query: infer B extends {} } } ? B
    : never;
type Produces<P extends keyof paths, M extends keyof paths[P]> =
  paths[P][M] extends { responses: infer R } ? {
      [C in keyof R]: Values<Values<R[C]>>;
    }
    : never;

export const API_BASE = "http://localhost:3000";

export const send = <
  P extends keyof paths,
  M extends string & keyof paths[P],
  B extends "requestBody" extends keyof paths[P][M] ? "json"
    : "parameters" extends keyof paths[P][M] ? "parameters"
    : never,
>(
  path: P,
  method: M,
  kind: B,
  content: Props<P, M>,
): {
  data: Promise<Produces<P, M>>;
  cancel: (reason?: string) => void;
} => {
  const [params, body, headers] = kind == "json"
    ? ["", JSON.stringify(content), { "Content-Type": "application/json" }]
    : [
      `?${new URLSearchParams(content)}`,
      void 0,
      {},
    ];

  let inFlight = true;
  const controller = new AbortController();
  const data = fetch(`${API_BASE}${path}${params}`, {
    method: method.toUpperCase(),
    body,
    signal: controller.signal,
    headers,
  }).then(async (res) => {
    inFlight = false;
    if (res.ok) {
      const text = await res.text();
      try {
        return { [res.status]: JSON.parse(text) } as Produces<P, M>;
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

type SSEStream<P extends keyof paths, M extends keyof paths[P]> =
  Produces<P, M> extends { [200]: infer D } ? (
      event: { type: "message"; data: D } | {
        type: "error";
        event: Event;
      },
    ) => void
    : never;

const sse = <
  P extends keyof paths,
  M extends string & keyof paths[P],
  B extends "requestBody" extends keyof paths[P][M] ? "json"
    : "parameters" extends keyof paths[P][M] ? "parameters"
    : never,
>(
  path: P,
  kind: B,
  content: Props<P, M>,
): {
  cancel: () => void;
  listen: (stream: SSEStream<P, M>) => void;
} => {
  const [params] = kind == "json" ? [""] : [
    `?${new URLSearchParams(content)}`,
  ];

  const source = new EventSource(`${API_BASE}${path}${params}`);

  let stream: SSEStream<P, M> | null = null;

  source.onmessage = (event) => {
    const data = event.data;
    stream?.({ type: "message", data });
  };
  source.onerror = (event) => {
    stream?.({ type: "error", event });
  };
  return {
    cancel: () => source.close(),
    listen: (newStream) => stream = newStream,
  };
};

export const api = {
  search: (props: Props<"/beta/api/search", "post">) => {
    const { data, cancel } = send("/beta/api/search", "post", "json", props);
    return { data: data.then((res) => res[200]), cancel };
  },
  autosuggest: (props: Props<"/beta/api/autosuggest", "post">) => {
    const { data, cancel } = send(
      "/beta/api/autosuggest",
      "post",
      "parameters",
      props,
    );
    return { data: data.then((res) => res[200]), cancel };
  },
  similarSites: (props: Props<"/beta/api/webgraph/similar_sites", "post">) => {
    const { data, cancel } = send(
      "/beta/api/webgraph/similar_sites",
      "post",
      "json",
      props,
    );
    return { data: data.then((res) => res[200]), cancel };
  },
  knowsSite: (props: Props<"/beta/api/webgraph/knows_site", "post">) => {
    const { data, cancel } = send(
      "/beta/api/webgraph/knows_site",
      "post",
      "parameters",
      props,
    );
    return { data: data.then((res) => res[200]), cancel };
  },
  factCheck: (props: Props<"/beta/api/fact_check", "post">) => {
    const { data, cancel } = send(
      "/beta/api/fact_check",
      "post",
      "json",
      props,
    );
    return { data: data.then((res) => res[200]), cancel };
  },
  alice: (
    props: Props<"/beta/api/alice", "get">,
    stream: (
      msg: { type: "begin" } | { type: "content"; data: ExecutionState } | {
        type: "done";
      },
    ) => void,
  ) => {
    const { cancel, listen } = sse("/beta/api/alice", "parameters", props);
    stream({ type: "begin" });
    listen((e) => {
      match(e)
        .with(
          { type: "message" },
          ({ data }) => stream({ type: "content", data }),
        )
        .with({ type: "error" }, () => {
          stream({ type: "done" });
          cancel();
        })
        .exhaustive();
    });
  },
  summarize: (
    props: Props<"/beta/api/summarize", "get">,
    stream: (
      msg: { type: "begin" } | { type: "content"; data: string } | {
        type: "done";
      },
    ) => void,
  ) => {
    const { cancel, listen } = sse("/beta/api/summarize", "parameters", props);
    stream({ type: "begin" });
    listen((e) => {
      match(e)
        .with(
          { type: "message" },
          ({ data }) => stream({ type: "content", data }),
        )
        .with({ type: "error" }, () => {
          stream({ type: "done" });
          cancel();
        })
        .exhaustive();
    });
  },

  queryId: (
    { query, urls }: { query: string; urls: string[] },
  ) => {
    const { signal, abort } = new AbortController();
    let finished = false;
    const data = fetch(`${API_BASE}/improvement/store`, {
      method: "POST",
      signal,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        "query": query,
        "urls": urls,
      }),
    }).then((response) => response.text()).then((data) => {
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
  sendImprovementClick: (
    { queryId, click }: { queryId: string; click: string },
  ) => {
    window.navigator.sendBeacon(
      `${API_BASE}/improvement/click?qid=${queryId}&click=${click}`,
    );
  },
};

export type SearchResults = components["schemas"]["ApiSearchResult"];
export type WebsitesResult = components["schemas"]["WebsitesResult"];
export type Webpage = components["schemas"]["DisplayedWebpage"];
export type Sidebar = components["schemas"]["Sidebar"];
export type Entity = components["schemas"]["DisplayedEntity"];
export type Widget = components["schemas"]["Widget"];
export type DisplayedAnswer = components["schemas"]["DisplayedAnswer"];
export type ScoredSite = components["schemas"]["ScoredSite"];
export type Suggestion = components["schemas"]["Suggestion"];

export type StackOverflowAnswer = components["schemas"]["StackOverflowAnswer"];
export type StackOverflowQuestion =
  components["schemas"]["StackOverflowQuestion"];

export type Region = components["schemas"]["Region"];
export const ALL_REGIONS = [
  "All",
  "Denmark",
  "France",
  "Germany",
  "Spain",
  "US",
] satisfies Region[];

export type FactCheckResponse = components["schemas"]["FactCheckResponse"];
export type ExecutionState = components["schemas"]["ExecutionState"];
