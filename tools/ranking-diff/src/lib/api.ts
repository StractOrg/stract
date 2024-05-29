import type { Experiment, Query } from '$lib';
import type { SimpleWebpage } from './webpage';

export type ApiOptions = {
  fetch?: typeof fetch;
};

export const fetchExperimentById = async (
  id: number,
  options?: ApiOptions,
): Promise<Experiment> => {
  const res = await (options?.fetch ?? fetch)(`/api/experiments/by_id`, {
    method: 'POST',
    body: JSON.stringify({
      id,
    }),
  });

  const experiment = (await res.json()) as Experiment;

  return experiment;
};

export const fetchQueryById = async (id: number, options?: ApiOptions): Promise<Query> => {
  const res = await (options?.fetch ?? fetch)(`/api/queries/by_id`, {
    method: 'POST',
    body: JSON.stringify({
      id,
    }),
  });

  const query = (await res.json()) as Query;

  return query;
};

export const fetchSerpByQueryAndExperiment = async (
  queryId: number,
  experimentId: number,
  options?: ApiOptions,
): Promise<SimpleWebpage[]> => {
  const res = await (options?.fetch ?? fetch)(`/api/experiments/get_serp`, {
    method: 'POST',
    body: JSON.stringify({
      queryId,
      experimentId,
    }),
  });

  const serp = (await res.json()) as SimpleWebpage[];

  return serp;
};
