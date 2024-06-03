import { fetchExperimentById, fetchAllCategories } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async (req) => {
  const { params, fetch } = req;

  const categories = await fetchAllCategories({ fetch });

  const baseline = await fetchExperimentById(Number(params.baseline), { fetch });
  const experiment = await fetchExperimentById(Number(params.experiment), { fetch });

  return {
    baseline,
    experiment,
    categories,
  };
};
