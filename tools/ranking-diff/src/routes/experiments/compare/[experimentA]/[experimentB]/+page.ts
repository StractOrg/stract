import type { Experiment, Query } from '$lib';
import type { PageLoad } from './$types';

export const load: PageLoad = async (req) => {
  const { params, fetch } = req;

  let res = await fetch(`/api/experiments/queries/intersection`, {
    method: 'POST',
    body: JSON.stringify({
      experimentA: params.experimentA,
      experimentB: params.experimentB,
    }),
  });

  let queries = (await res.json()) as Query[];

  res = await fetch(`/api/experiments/by_id`, {
    method: 'POST',
    body: JSON.stringify({
      id: params.experimentA,
    }),
  });

  const experimentA = (await res.json()) as Experiment;

  res = await fetch(`/api/experiments/by_id`, {
    method: 'POST',
    body: JSON.stringify({
      id: params.experimentB,
    }),
  });

  const experimentB = (await res.json()) as Experiment;

  return {
    experimentA,
    experimentB,
    queries,
  };
};
