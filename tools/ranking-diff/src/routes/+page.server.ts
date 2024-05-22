import { getExperiments, getQueries } from '$lib/db';

export const load = async () => {
  const queries = getQueries();
  const experiments = getExperiments();

  return { queries, experiments };
};
