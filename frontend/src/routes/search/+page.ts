import { redirect } from '@sveltejs/kit';
import type { PageLoad } from './$types';
import { match } from 'ts-pattern';
import { extractSearchParams } from '$lib/search';
import { globals } from '$lib/globals';

export const load: PageLoad = async (req) => {
  const { url } = req;
  let params = extractSearchParams(url.searchParams);

  if (!params.query.trim()) {
    const form = req.data['form'];
    if (form) {
      params = form;
    } else {
      redirect(301, '/');
    }
  }

  if (!params.query.trim()) {
    redirect(301, '/');
  }

  const results = req.data['results'];

  if (results.type == 'bang') {
    redirect(301, results.redirectTo);
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
    globals: await globals({
      title: `${params.query} – Stract`,
      header: { divider: true },
    }),
  };
};
