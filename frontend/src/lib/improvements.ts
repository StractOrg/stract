import type { Action } from 'svelte/action';
import { writable } from 'svelte/store';
import { api, type Webpage } from './api';
import { allowStatsStore } from './stores';

const queryIdStore = writable<string | undefined>();

export const updateQueryId = async ({ query, webpages }: { query: string; webpages: Webpage[] }) =>
  queryIdStore.set(await api.queryId({ query, urls: webpages.map((wp) => wp.url) }).data);

export const improvements: Action<HTMLAnchorElement, Webpage> = (node, webpage) => {
  let queryId: string | undefined;
  let allowStats: boolean | undefined;

  queryIdStore.subscribe((id) => (queryId = id));
  allowStatsStore.subscribe((allow) => (allowStats = allow));

  const listener = () => {
    if (!queryId || !allowStats) return;
    api.sendImprovementClick({ queryId, click: webpage.url });
  };

  node.addEventListener('click', listener);

  return {
    destroy: () => node.removeEventListener('click', listener),
  };
};
