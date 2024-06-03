<script lang="ts">
  import type { PageData } from './$types';
  import BackIcon from '~icons/heroicons/arrow-left';
  import LikeIcon from '~icons/heroicons/hand-thumb-up';
  import Serp from './Serp.svelte';
  import { like, likedState, unlike } from '$lib/api';
  import { browser } from '$app/environment';
  import Settings from '../../../../../components/Settings.svelte';
  import { shuffleExperimentsStore } from '$lib/stores';
  import type { SimpleWebpage } from '$lib/webpage';
  import type { Experiment } from '$lib';
  import type { LikedState } from '$lib/db';

  export let data: PageData;

  $: baseline = data.baseline;
  $: experiment = data.experiment;
  $: query = data.query;
  $: allQueries = data.allQueries;

  $: queryIndex = allQueries.findIndex((q) => q.id === query.id);
  $: prevQuery = allQueries[queryIndex - 1];
  $: nextQuery = allQueries[queryIndex + 1];

  $: experiments = [
    { isBaseline: true, ...baseline },
    { isBaseline: false, ...experiment },
  ];

  $: {
    if ($shuffleExperimentsStore && browser && experiments) {
      experiments = experiments
        .map((value) => ({ value, sort: Math.random() }))
        .sort((a, b) => a.sort - b.sort)
        .map(({ value }) => value);
    } else {
      experiments = [
        { isBaseline: true, ...baseline },
        { isBaseline: false, ...experiment },
      ];
    }

    if (browser && experiments) {
      updateLikedExperiment();
    }
  }

  $: likedExperiment = null as number | null;

  const updateLikedExperiment = async () => {
    const state = await likedState(baseline.experiment.id, experiment.experiment.id, query.id, {
      fetch,
    });
    if (state === 'baseline') {
      likedExperiment = baseline.experiment.id;
    } else if (state === 'experiment') {
      likedExperiment = experiment.experiment.id;
    } else {
      likedExperiment = null;
    }
  };

  const unlikeAllExperiments = async () => {
    await unlike(baseline.experiment.id, experiment.experiment.id, query.id, { fetch });
  };

  const likeExperiment = async (e: {
    experiment: Experiment;
    isBaseline: boolean;
    serp: SimpleWebpage[];
  }) => {
    await unlikeAllExperiments();

    const wasLiked = likedExperiment === e.experiment.id;

    if (!wasLiked) {
      if (e.isBaseline) {
        await like(baseline.experiment.id, experiment.experiment.id, query.id, 'baseline', {
          fetch,
        });
      } else {
        await like(baseline.experiment.id, experiment.experiment.id, query.id, 'experiment', {
          fetch,
        });
      }
    }

    await updateLikedExperiment();
  };

  const onKeyDown = (event: KeyboardEvent) => {
    if (event.key === 'ArrowLeft' && prevQuery) {
      window.location.href = prevQuery.id.toString();
    } else if (event.key === 'ArrowRight' && nextQuery) {
      window.location.href = nextQuery.id.toString();
    } else if (event.key.toLowerCase() == 'a') {
      likeExperiment(experiments[0]);
    } else if (event.key.toLowerCase() == 'b') {
      likeExperiment(experiments[1]);
    }
  };
</script>

<svelte:window on:keydown={onKeyDown} />

<div class="flex w-full justify-between">
  <div class="w-fit">
    <a href="/experiments/compare/{baseline.experiment.id}/{experiment.experiment.id}">
      <BackIcon class="h-6 w-6" />
    </a>
  </div>
  <div><Settings /></div>
</div>
<div>
  <h1 class="grow text-center text-lg font-semibold">{query.text}</h1>
</div>

<div class="mt-10 flex h-3/4 w-full justify-center overflow-auto">
  <div class="flex w-full max-w-5xl gap-x-2">
    <div class="w-full">
      <h2 class="mb-5 text-center">A</h2>
      <Serp webpages={experiments[0].serp} />
    </div>
    <div class="w-full">
      <h2 class="mb-5 text-center">B</h2>
      <Serp webpages={experiments[1].serp} />
    </div>
  </div>
</div>

<div class="mt-5 flex w-full justify-center">
  <div class="flex w-full max-w-5xl">
    {#each experiments as e}
      <div class="flex grow justify-center">
        <button
          on:click={() => likeExperiment(e)}
          class="{likedExperiment && likedExperiment == e.experiment.id
            ? 'text-green-500'
            : 'text-slate-300'} hover:text-green-700"><LikeIcon class="h-6 w-6" /></button
        >
      </div>
    {/each}
  </div>
</div>

<div class="mt-10 flex w-full justify-between">
  {#if prevQuery}
    <a href={prevQuery.id.toString()}>Prev</a>
  {:else}
    <span class="text-gray-400">Prev</span>
  {/if}
  {#if nextQuery}
    <a href={nextQuery.id.toString()}>Next</a>
  {:else}
    <span class="text-gray-400">Next</span>
  {/if}
</div>
