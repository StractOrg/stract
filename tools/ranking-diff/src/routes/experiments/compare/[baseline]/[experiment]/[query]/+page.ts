import type { ExperimentResult } from '$lib';
import {
  fetchExperimentById,
  fetchQueriesIntersection,
  fetchQueryById,
  fetchSerpByQueryAndExperiment,
} from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async (req) => {
  const { params, fetch } = req;

  const query = await fetchQueryById(Number(params.query), { fetch });

  const baseline = await fetchExperimentById(Number(params.baseline), { fetch });
  const baselineSerp = await fetchSerpByQueryAndExperiment(
    Number(params.query),
    Number(params.baseline),
    { fetch },
  );

  const experiment = await fetchExperimentById(Number(params.experiment), { fetch });
  const experimentSerp = await fetchSerpByQueryAndExperiment(
    Number(params.query),
    Number(params.experiment),
    { fetch },
  );

  const allQueries = await fetchQueriesIntersection(
    Number(params.baseline),
    Number(params.experiment),
    { fetch },
  );

  return {
    baseline: { experiment: baseline, serp: baselineSerp } as ExperimentResult,
    experiment: { experiment, serp: experimentSerp } as ExperimentResult,
    query,
    allQueries,
  };
};
