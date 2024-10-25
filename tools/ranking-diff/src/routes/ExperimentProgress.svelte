<script lang="ts">
	import type { Experiment, Query } from '$lib';
	import { asSimpleWebpage, type SimpleWebpage, type Webpage } from '$lib/webpage';
	import { createEventDispatcher, onMount } from 'svelte';
	import { tweened } from 'svelte/motion';
	import { searchApiStore } from '$lib/stores';

	export let experiment: Experiment;
	export let queries: Query[];

	const REQ_DELAY_MS = 1000;

	let currentQuery = 0;

	const search = async (query: Query): Promise<SimpleWebpage[]> => {
		const res = await fetch($searchApiStore, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ query: query.text, returnRankingSignals: true })
		});

		const data = await res.json();
		const webpages = data.webpages as Webpage[];

		return webpages.map((webpage) => asSimpleWebpage(webpage));
	};

	const addSerpToExperiment = async (query: Query, pages: SimpleWebpage[]) => {
		await fetch('/api/experiments/add_serp', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				experimentId: experiment.id,
				queryId: query.id,
				webpages: pages
			})
		});
	};

	onMount(async () => {
		for (let i = 0; i < queries.length; i++) {
			currentQuery = i;
			progress.set(((i + 1) / queries.length) * 100);

			const query = queries[i];
			const pages = await search(query);
			await addSerpToExperiment(query, pages);

			await new Promise((resolve) => setTimeout(resolve, REQ_DELAY_MS));
		}

		dispatch('finish');
	});

	const dispatch = createEventDispatcher();
	const progress = tweened(0, { duration: 100 });
</script>

<div>
	<span class="flex w-full gap-x-2">
		<p class="text-sm">{$progress.toFixed(0)}%</p>
		<progress max="100" value={$progress}></progress>
	</span>
	<p class="text-sm">processing: <i>{queries[currentQuery].text}</i>...</p>
</div>
