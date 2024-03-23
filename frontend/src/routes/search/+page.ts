import { redirect } from '@sveltejs/kit';
import type { PageLoad } from './$types';
import { extractSearchParams, search, type SearchResults } from '$lib/search';
import { globals } from '$lib/globals';
import { browser } from '$app/environment';

export const load: PageLoad = async (req) => {
  const { url, fetch } = req;
  const { clientAddress, form } = req.data;

  const performSsr = url.searchParams.get('ssr') === 'true';

  let params = extractSearchParams(url.searchParams);

  if (!params.query.trim()) {
    if (form) {
      params = form;
    } else {
      redirect(301, '/');
    }
  }

  if (!params.query.trim()) {
    redirect(301, '/');
  }

  let results: SearchResults | null = null;

  if (performSsr && !browser) {
    results = await search(params, {
      fetch,
      headers: { 'X-Forwarded-For': clientAddress },
    });
  }

  if (results && results.type == 'bang') {
    redirect(301, results.redirectTo);
  }

  return {
    params,
    results,
    globals: await globals({
      title: `${params.query} – Stract`,
      header: { divider: true },
    }),
  };
};
