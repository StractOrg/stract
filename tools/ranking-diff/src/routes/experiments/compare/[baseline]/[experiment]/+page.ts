import type { Query } from '$lib';
import { fetchExperimentById, fetchQueriesIntersection } from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async (req) => {
	const { params, fetch } = req;

	const queries = await fetchQueriesIntersection(
		Number(params.baseline),
		Number(params.experiment),
		{ fetch }
	);

	const baseline = await fetchExperimentById(Number(params.baseline), { fetch });
	const experiment = await fetchExperimentById(Number(params.experiment), { fetch });

	return {
		baseline,
		experiment,
		queries
	};
};
