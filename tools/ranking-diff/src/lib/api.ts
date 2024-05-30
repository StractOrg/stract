import type { Category, Experiment, Query, LikedState } from '$lib';
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

export const fetchAllCategories = async (options?: ApiOptions): Promise<Category[]> => {
  const res = await (options?.fetch ?? fetch)(`/api/categories`);

  const categories = (await res.json()) as Category[];

  return categories;
};

export const fetchQueriesByCategory = async (
  categoryId: number,
  options?: ApiOptions,
): Promise<Query[]> => {
  const res = await (options?.fetch ?? fetch)(`/api/categories/get_queries`, {
    method: 'POST',
    body: JSON.stringify({
      categoryId,
    }),
  });

  const queries = (await res.json()) as Query[];

  return queries;
};

export const like = async (
  baselineId: number,
  experimentId: number,
  queryId: number,
  state: LikedState,
  options?: ApiOptions,
) => {
  await (options?.fetch ?? fetch)(`/api/experiments/like`, {
    method: 'POST',
    body: JSON.stringify({
      baselineId,
      experimentId,
      queryId,
      state,
    }),
  });
};

export const unlike = async (
  baselineId: number,
  experimentId: number,
  queryId: number,
  options?: ApiOptions,
) => {
  await (options?.fetch ?? fetch)(`/api/experiments/unlike`, {
    method: 'POST',
    body: JSON.stringify({
      baselineId,
      experimentId,
      queryId,
    }),
  });
};

export const likedState = async (
  baselineId: number,
  experimentId: number,
  queryId: number,
  options?: ApiOptions,
): Promise<LikedState> => {
  const res = await (options?.fetch ?? fetch)(`/api/experiments/liked_state`, {
    method: 'POST',
    body: JSON.stringify({
      baselineId,
      experimentId,
      queryId,
    }),
  });

  const state = (await res.json()) as LikedState;

  return state;
};
