import type { Query } from '$lib';
import { fetchExperimentById } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async (req) => {
  const { params, fetch } = req;

  let res = await fetch(`/api/experiments/queries/intersection`, {
    method: 'POST',
    body: JSON.stringify({
      experimentA: params.baseline,
      experimentB: params.experiment,
    }),
  });

  let queries = (await res.json()) as Query[];

  const baseline = await fetchExperimentById(Number(params.baseline), { fetch });
  const experiment = await fetchExperimentById(Number(params.experiment), { fetch });

  return {
    baseline,
    experiment,
    queries,
  };
};
