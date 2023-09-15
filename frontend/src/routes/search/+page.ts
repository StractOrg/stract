import { api, type Region } from '$lib/api';
import { redirect } from '@sveltejs/kit';
import type { PageLoad } from './$types';
import { fetchRemoteOptic } from '$lib/optics';
import { match } from 'ts-pattern';
import { decompressRanked, type RankedSites } from '$lib/rankings';
import { globals } from '$lib/globals';

export const load: PageLoad = async ({ fetch, url }) => {
  const params = extractSearchParams(url.searchParams);

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

type SearchParams = {
  query: string;
  currentPage: number;
  optic: string | undefined;
  selectedRegion: Region | undefined;
  safeSearch: boolean;
  compressedSiteRankings: string | null;
  siteRankings: RankedSites | undefined;
};

const extractSearchParams = (searchParams: URLSearchParams): SearchParams => {
  const query = searchParams.get('q') ?? '';
  const currentPage = parseInt(searchParams.get('p') ?? '1') || 1;
  const optic = searchParams.get('optic') || void 0;
  const selectedRegion = (searchParams.get('gl') || void 0) as Region | undefined;
  const safeSearch = searchParams.get('ss') == 'true';
  const compressedSiteRankings = searchParams.get('sr');
  const siteRankings = compressedSiteRankings ? decompressRanked(compressedSiteRankings) : void 0;

  return {
    query,
    currentPage,
    optic,
    selectedRegion,
    safeSearch,
    compressedSiteRankings,
    siteRankings,
  };
};
