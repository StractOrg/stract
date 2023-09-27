import { api } from '$lib/api';
import { redirect } from '@sveltejs/kit';
import type { PageLoad } from './$types';
import { fetchRemoteOptic } from '$lib/optics';
import { match } from 'ts-pattern';
import { extractSearchParams } from '$lib/search';
import { globals } from '$lib/globals';

export const load: PageLoad = async (req) => {
  const { fetch, url } = req;
  let params = extractSearchParams(url.searchParams);

  if (!params.query.trim()) {
    const form = req.data['form'];
    if (form) {
      params = form;
    } else {
      throw redirect(300, '/');
    }
  }

  if (!params.query.trim()) {
    throw redirect(300, '/');
  }

  const { data } = api.search(
    {
      query: params.query,
      page: params.currentPage - 1,
      safeSearch: params.safeSearch,
      optic: params.optic && (await fetchRemoteOptic({ opticUrl: params.optic, fetch })),
      selectedRegion: params.selectedRegion,
      siteRankings: params.siteRankings,
      fetchDiscussions: true,
    },
    { fetch },
  );
  const results = await data;

  if (results.type == 'bang') {
    throw redirect(300, results.redirectTo);
  }

  const prevPageSearchParams = match(params.currentPage > 1)
    .with(true, () => {
      const newParams = new URLSearchParams(url.searchParams);
      newParams.set('p', (params.currentPage - 1).toString());
      return newParams;
    })
    .otherwise(() => {});
  const nextPageSearchParams = match(results.type == 'websites' && results.hasMoreResults)
    .with(true, () => {
      const newParams = new URLSearchParams(url.searchParams);
      newParams.set('p', (params.currentPage + 1).toString());
      return newParams;
    })
    .otherwise(() => {});

  return {
    ...params,
    prevPageSearchParams,
    nextPageSearchParams,
    results,
    globals: globals({
      title: `${params.query} – Stract`,
      header: { divider: true },
    }),
  };
};
