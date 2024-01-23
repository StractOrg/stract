import { discussionsOptic, extractSearchParams, type SearchParams, type SearchResults } from '$lib/search';
import { redirect } from '@sveltejs/kit';
import type { Actions } from './$types';
import { api } from '$lib/api';
import { fetchRemoteOptic } from '$lib/optics';

export const load = async ({ locals, fetch, url, getClientAddress }) => {
  const searchParams: SearchParams | undefined =
    (locals['form'] && extractSearchParams(locals['form'])) || undefined;

  let params = extractSearchParams(url.searchParams);

  if (!params.query.trim()) {
    const form = searchParams;
    if (form) {
      params = form;
    } else {
      redirect(301, '/');
    }
  }

  if (!params.query.trim()) {
    redirect(301, '/');
  }

  let start = Date.now();

  const { data: websitesReq } = api.search(
    {
      query: params.query,
      page: params.currentPage - 1,
      safeSearch: params.safeSearch,
      optic: params.optic && (await fetchRemoteOptic({ opticUrl: params.optic, fetch })),
      selectedRegion: params.selectedRegion,
      hostRankings: params.host_rankings,
      countResults: true,
    },
    { fetch, headers: { 'X-Forwarded-For': getClientAddress() } },
  );

  const { data: widgetReq } = params.currentPage == 1 ?
  api.searchWidget(
    {
      query: params.query,
    },
    { fetch, headers: { 'X-Forwarded-For': getClientAddress() } },
  )
  : { data: undefined };

  const { data: sidebarReq } = params.currentPage == 1 ? 
  api.searchSidebar(
    {
      query: params.query,
    },
    { fetch, headers: { 'X-Forwarded-For': getClientAddress() } },
  )
  : { data: undefined };

  const { data: discussionsReq } = params.currentPage == 1 ?
  api.search(
    {
      query: params.query,
      optic: discussionsOptic,
      numResults: 10,
      safeSearch: params.safeSearch,
      selectedRegion: params.selectedRegion,
      hostRankings: params.host_rankings,
      returnRankingSignals: true,
      countResults: false,
    },
    { fetch, headers: { 'X-Forwarded-For': getClientAddress() } },
  )
  : { data: undefined };

  const [websites, widget, sidebar, discussionsRes] = await Promise.all([websitesReq, widgetReq, sidebarReq, discussionsReq]);
  const discussions = discussionsRes?.type == "websites" ? discussionsRes.webpages : undefined;

  let results: SearchResults = websites.type == "websites" ? {
    ...websites,
    widget,
    sidebar,
    discussions,
  } : {
    ...websites,
  };

  if (results.type == "websites") {
    results.searchDurationMs = Date.now() - start;
  }

  return { form: searchParams, results };
};


export const actions: Actions = {
  default: async (event) => {
    const { request } = event;

    event.locals.form = await request.formData();

    return { success: true };
  },
} satisfies Actions;
