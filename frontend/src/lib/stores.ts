import { browser } from '$app/environment';
import { writable } from 'svelte/store';
import type { OpticOption } from './optics';
import type { SiteRakings } from './rankings';
import { api, type Webpage } from './api';
import { match } from 'ts-pattern';

const parseJSONWithFallback = <T>(json: string, fallback: T, message = '') => {
  try {
    return JSON.parse(json);
  } catch (e) {
    if (message) console.warn(message, { json });
    return fallback;
  }
};

const writableLocalStorage = <T>(
  key: string,
  defaultValue: T,
  storage = browser && localStorage,
) => {
  const storedValue = storage && storage.getItem(key);
  const store = writable<T>(
    typeof storedValue == 'string'
      ? parseJSONWithFallback(storedValue, defaultValue, `Failed to parse value stored in '${key}'`)
      : defaultValue,
  );
  store.subscribe(($value) => {
    if (storage) storage.setItem(key, JSON.stringify($value));
  });
  return store;
};

const SAFE_SEARCH_KEY = 'safeSearch';
export const safeSearchStore = writableLocalStorage<boolean>(SAFE_SEARCH_KEY, false);

const OPTICS_KEY = 'optics';
export const opticsStore = writableLocalStorage<OpticOption[]>(OPTICS_KEY, []);

const ALLOW_STATS_KEY = 'allowStats';
export const allowStatsStore = writableLocalStorage<boolean>(ALLOW_STATS_KEY, true);

const SITE_RANKINGS_KEY = 'siteRankings';
export const siteRankingsStore = writableLocalStorage<SiteRakings>(SITE_RANKINGS_KEY, {});

const SEARCH_QUERY_KEY = 'searchQuery';
export const searchQueryStore = writableLocalStorage<string | undefined>(
  SEARCH_QUERY_KEY,
  void 0,
  browser && sessionStorage,
);

export const summariesStore = writable<
  Record<string, { inProgress: boolean; data: string } | undefined>
>({});

// Actions

export const summarize = (query: string, site: Webpage) => {
  api.summarize({ query, url: site.url }, (e) => {
    summariesStore.update(($summaries) =>
      match(e)
        .with({ type: 'begin' }, () => ({
          ...$summaries,
          [site.url]: { inProgress: true, data: '' },
        }))
        .with({ type: 'content' }, ({ data }) => ({
          ...$summaries,
          [site.url]: {
            inProgress: true,
            data: ($summaries[site.url]?.data ?? '') + data,
          },
        }))
        .with({ type: 'done' }, () => ({
          ...$summaries,
          [site.url]: {
            inProgress: false,
            data: $summaries[site.url]?.data ?? '',
          },
        }))
        .exhaustive(),
    );
  });
};
export const clearSummary = (site: Webpage) => {
  summariesStore.update(($summaries) => ({ ...$summaries, [site.url]: void 0 }));
};
