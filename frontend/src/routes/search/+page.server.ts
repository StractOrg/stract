import { extractSearchParams, type SearchParams } from '$lib/search';
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
      throw redirect(301, '/');
    }
  }

  if (!params.query.trim()) {
    throw redirect(301, '/');
  }

  const { data } = api.search(
    {
      query: params.query,
      page: params.currentPage - 1,
      safeSearch: params.safeSearch,
      optic: params.optic && (await fetchRemoteOptic({ opticUrl: params.optic, fetch })),
      selectedRegion: params.selectedRegion,
      siteRankings: params.siteRankings,
      fetchDiscussions: false,
      countResults: true,
    },
    { fetch, headers: { 'X-Forwarded-For': getClientAddress() } },
  );

  const results = await data;

  return { form: searchParams, results };
};

export const actions: Actions = {
  default: async (event) => {
    const { request } = event;

    event.locals.form = await request.formData();

    return { success: true };
  },
} satisfies Actions;
