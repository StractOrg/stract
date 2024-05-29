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

export const fetchQueriesIntersection = async (
  experimentA: number,
  experimentB: number,
  options?: ApiOptions,
): Promise<Query[]> => {
  const res = await (options?.fetch ?? fetch)(`/api/experiments/queries/intersection`, {
    method: 'POST',
    body: JSON.stringify({
      experimentA,
      experimentB,
    }),
  });

  const queries = (await res.json()) as Query[];

  return queries;
};

export const like = async (experimentId: number, queryId: number, options?: ApiOptions) => {
  await (options?.fetch ?? fetch)(`/api/experiments/like`, {
    method: 'POST',
    body: JSON.stringify({
      experimentId,
      queryId,
    }),
  });
};

export const unlike = async (experimentId: number, queryId: number, options?: ApiOptions) => {
  await (options?.fetch ?? fetch)(`/api/experiments/unlike`, {
    method: 'POST',
    body: JSON.stringify({
      experimentId,
      queryId,
    }),
  });
};

export const isLiked = async (
  experimentId: number,
  queryId: number,
  options?: ApiOptions,
): Promise<boolean> => {
  const res = await (options?.fetch ?? fetch)(`/api/experiments/is_liked`, {
    method: 'POST',
    body: JSON.stringify({
      experimentId,
      queryId,
    }),
  });

  const liked = (await res.json()) as boolean;

  return liked;
};
