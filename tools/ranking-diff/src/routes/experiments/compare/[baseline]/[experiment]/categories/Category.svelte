<script lang="ts">
  import type { Category, Experiment, Query, LikedState } from '$lib';
  import { fetchQueriesByCategory, likedState } from '$lib/api';
  import { onMount } from 'svelte';
  import Gauge from './Gauge.svelte';
  import QueryComponent from '../Query.svelte';

  export let category: Category;
  export let baseline: Experiment;
  export let experiment: Experiment;

  $: queries = [] as Query[];
  $: states = [] as LikedState[];

  onMount(async () => {
    queries = await fetchQueriesByCategory(category.id);
    states = await Promise.all(
      queries.map((query) => likedState(baseline.id, experiment.id, query.id)),
    );
  });

  $: score = states.reduce((acc, state) => {
    if (state === 'baseline') {
      return acc - 1;
    } else if (state === 'experiment') {
      return acc + 1;
    }
    return acc;
  }, 0);

  $: annotations = states.filter((state) => state !== 'none').length;

  let showQueries = false;
</script>

<div class="flex flex-col items-center">
  <button class="flex" on:click={() => (showQueries = !showQueries)}>
    {category.name}
    {annotations} / {queries.length}
  </button>

  {#if score < 0}
    <span class="text-xs text-red-500">{score} ↓</span>
  {:else if score > 0}
    <span class="text-xs text-green-500">{score} ↑</span>
  {:else}
    <span class="text-xs text-gray-400">no change</span>
  {/if}

  <Gauge value={score} min={-queries.length} max={queries.length} />

  {#if showQueries}
    <div class="mt-5 grid w-full grid-cols-5 gap-2">
      {#each queries as query}
        <QueryComponent {query} {experiment} {baseline} />
      {/each}
    </div>
  {/if}
</div>
