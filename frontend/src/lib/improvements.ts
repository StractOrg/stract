import type { Action } from 'svelte/action';
import { getApiBase, type ApiOptions, type DisplayedWebpage, requestPlain } from './api';
import { allowStatsStore, queryIdStore } from './stores';

export const updateQueryId = async ({
  query,
  webpages,
}: {
  query: string;
  webpages: DisplayedWebpage[];
}) => {
  let allowStats: boolean | undefined;
  allowStatsStore.subscribe((allow) => (allowStats = allow));

  if (!allowStats) return;

  queryIdStore.set(await queryId({ query, urls: webpages.map((wp) => wp.url) }).data);
};

export const improvements: Action<HTMLAnchorElement, number> = (node, webpageIndex) => {
  let queryId: string | undefined;
  let allowStats: boolean | undefined;

  queryIdStore.subscribe((id) => (queryId = id));
  allowStatsStore.subscribe((allow) => (allowStats = allow));

  const listener = () => {
    if (!queryId || !allowStats) return;
    sendImprovementClick({ queryId, clickIndex: webpageIndex });
  };

  node.addEventListener('click', listener);

  return {
    destroy: () => node.removeEventListener('click', listener),
  };
};

const queryId = ({ query, urls }: { query: string; urls: string[] }, options?: ApiOptions) =>
  requestPlain('POST', '/improvement/store', { query, urls }, options);

const sendImprovementClick = (
  { queryId, clickIndex }: { queryId: string; clickIndex: number },
  options?: ApiOptions,
) => {
  window.navigator.sendBeacon(
    `${getApiBase(options)}/improvement/click?qid=${queryId}&click=${clickIndex}`,
  );
};
