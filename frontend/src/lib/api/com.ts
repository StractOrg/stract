import { env } from '$env/dynamic/public';
import type { paths } from './schema.d.ts';

type Values<T> = T[keyof T];

export type Props<P extends keyof paths, M extends keyof paths[P]> = paths[P][M] extends {
  requestBody: { content: { 'application/json': infer B extends NonNullable<unknown> } };
}
  ? B
  : paths[P][M] extends { parameters: { path: infer B extends NonNullable<unknown> } }
  ? B
  : paths[P][M] extends { parameters: { query: infer B extends NonNullable<unknown> } }
  ? B
  : never;
export type Produces<P extends keyof paths, M extends keyof paths[P]> = paths[P][M] extends {
  responses: infer R;
}
  ? {
      [C in keyof R]: Values<Values<R[C]>>;
    }
  : never;

export const apiBaseFromEnv = () => env.PUBLIC_API_BASE || 'http://localhost:3000';
export const API_BASE = apiBaseFromEnv();

export type ApiOptions = {
  fetch?: typeof fetch;
};

export const send = <
  P extends keyof paths,
  M extends string & keyof paths[P],
  B extends 'requestBody' extends keyof paths[P][M]
    ? 'json'
    : 'parameters' extends keyof paths[P][M]
    ? 'parameters'
    : never,
>(
  path: P,
  method: M,
  kind: B,
  content: Props<P, M>,
  options: ApiOptions = {},
): {
  data: Promise<Produces<P, M>>;
  cancel: (reason?: string) => void;
} => {
  const [params, body, headers] =
    kind == 'json'
      ? ['', JSON.stringify(content), { 'Content-Type': 'application/json' }]
      : [`?${new URLSearchParams(content)}`, void 0, {}];

  let inFlight = true;
  const controller = new AbortController();
  const data = (options.fetch ?? fetch)(`${API_BASE}${path}${params}`, {
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

type SSEStream<P extends keyof paths, M extends keyof paths[P]> = Produces<P, M> extends {
  [200]: infer D;
}
  ? (
      event:
        | { type: 'message'; data: D }
        | {
            type: 'error';
            event: Event;
          },
    ) => void
  : never;

export const sse = <
  P extends keyof paths,
  M extends string & keyof paths[P],
  B extends 'requestBody' extends keyof paths[P][M]
    ? 'json'
    : 'parameters' extends keyof paths[P][M]
    ? 'parameters'
    : never,
>(
  path: P,
  kind: B,
  content: Props<P, M>,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  options?: ApiOptions,
): {
  cancel: () => void;
  listen: (stream: SSEStream<P, M>) => void;
} => {
  const [params] = kind == 'json' ? [''] : [`?${new URLSearchParams(content)}`];

  const source = new EventSource(`${API_BASE}${path}${params}`);

  let stream: SSEStream<P, M> | null = null;

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
