import { browser } from '$app/environment';
import { writable } from 'svelte/store';
import type { OpticOption } from './optics';
import type { SiteRakings } from './rankings';
import { api, type DisplayedWebpage } from './api';
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
    if (browser && storage) {
      if (typeof $value == 'undefined') {
        storage.removeItem(key);
      } else {
        storage.setItem(key, JSON.stringify($value));
      }
    }
  });

  const { set } = store;
  store.set = (value: T) => {
    if (storage) {
      set(value);
    }
  };

  return store;
};

const SAFE_SEARCH_KEY = 'safeSearch';
export const safeSearchStore = writableLocalStorage<boolean>(SAFE_SEARCH_KEY, false);

const POST_SEARCH_KEY = 'postSearch';
export const postSearchStore = writableLocalStorage<boolean>(POST_SEARCH_KEY, false);

const OPTICS_KEY = 'optics';
export const opticsStore = writableLocalStorage<OpticOption[]>(OPTICS_KEY, []);

const OPTICS_SHOW_KEY = 'opticsShow';
export const opticsShowStore = writableLocalStorage<Record<string, boolean>>(OPTICS_SHOW_KEY, {});

const ALLOW_STATS_KEY = 'allowStats';
export const allowStatsStore = writableLocalStorage<boolean>(ALLOW_STATS_KEY, true);

const SITE_RANKINGS_KEY = 'siteRankings';
export const siteRankingsStore = writableLocalStorage<SiteRakings>(SITE_RANKINGS_KEY, {});

const SEARCH_QUERY_KEY = 'searchQuery';
export const searchQueryStore = writableLocalStorage<string | undefined>(SEARCH_QUERY_KEY, void 0);

const QUERY_ID_KEY = 'queryId';
export const queryIdStore = writableLocalStorage<string | undefined>(QUERY_ID_KEY, void 0);


const THEME_KEY = 'theme';
export const themeStore = writableLocalStorage<string | void>(THEME_KEY, void 0);
if (browser)
  themeStore?.subscribe(($theme) => {
    const c = document.documentElement.className.replace(/theme-[^ ]+/, ``);
    const theme = $theme?.toLowerCase() || '';
    document.documentElement.className = `${c} ${theme}`.trim();
  });

type SummaryState = { inProgress: boolean; data: string } | undefined;
export const summariesStore = writable<Record<string, SummaryState>>({});

// Actions

export const summarize = (query: string, site: DisplayedWebpage) => {
  const updateSummary = (update: (summary: SummaryState) => SummaryState) => {
    summariesStore.update(($summaries) => ({
      ...$summaries,
      [site.url]: update($summaries[site.url]),
    }));
  };

  updateSummary(() => ({ inProgress: true, data: '' }));

  const { listen, cancel } = api.summarize({ query, url: site.url });

  listen((e) => {
    match(e)
      .with({ type: 'message' }, ({ data }) =>
        updateSummary((summary) => ({
          inProgress: true,
          data: (summary?.data ?? '') + data,
        })),
      )
      .with({ type: 'error' }, () => {
        cancel();
        updateSummary((summary) => ({
          inProgress: false,
          data: summary?.data ?? '',
        }));
      })
      .exhaustive();
  });
};

export const clearSummary = (site: DisplayedWebpage) => {
  summariesStore.update(($summaries) => ({ ...$summaries, [site.url]: void 0 }));
};
