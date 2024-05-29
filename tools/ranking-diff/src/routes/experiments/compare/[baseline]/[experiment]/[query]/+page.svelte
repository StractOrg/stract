<script lang="ts">
  import type { PageData } from './$types';
  import BackIcon from '~icons/heroicons/arrow-left';
  import LikeIcon from '~icons/heroicons/hand-thumb-up';
  import Serp from './Serp.svelte';
  import { isLiked, like, unlike } from '$lib/api';
  import { browser } from '$app/environment';
  import Settings from '../../../../../components/Settings.svelte';
  import { shuffleExperimentsStore } from '$lib/stores';

  export let data: PageData;

  $: baseline = data.baseline;
  $: experiment = data.experiment;
  $: query = data.query;
  $: allQueries = data.allQueries;

  $: queryIndex = allQueries.findIndex((q) => q.id === query.id);
  $: prevQuery = allQueries[queryIndex - 1];
  $: nextQuery = allQueries[queryIndex + 1];

  $: experiments = [baseline, experiment];

  $: {
    if ($shuffleExperimentsStore && browser && experiments) {
      experiments = experiments
        .map((value) => ({ value, sort: Math.random() }))
        .sort((a, b) => a.sort - b.sort)
        .map(({ value }) => value);
    } else {
      experiments = [baseline, experiment];
    }

    if (browser && experiments) {
      updateLikedExperiment();
    }
  }

  $: likedExperiment = null as number | null;

  const updateLikedExperiment = async () => {
    let newLikedExperiment = null;
    for (const experiment of experiments) {
      const liked = await experimentIsLiked(experiment.experiment.id);
      if (liked) {
        newLikedExperiment = experiment.experiment.id;
      }
    }

    likedExperiment = newLikedExperiment;
  };

  const experimentIsLiked = async (experimentId: number) =>
    await isLiked(experimentId, query.id, { fetch });

  const unlikeAllExperiments = async () => {
    for (const experiment of experiments) {
      await unlike(experiment.experiment.id, query.id, { fetch });
    }
  };

  const likeExperiment = async (experimentId: number) => {
    const wasLiked = likedExperiment === experimentId;

    await unlikeAllExperiments();

    if (!wasLiked) {
      await like(experimentId, query.id, { fetch });
    }

    await updateLikedExperiment();
  };
</script>

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
    {#each experiments as { serp }}
      <Serp webpages={serp} />
    {/each}
  </div>
</div>

<div class="mt-5 flex w-full justify-center">
  <div class="flex w-full max-w-5xl">
    {#each experiments as experiment}
      <div class="flex grow justify-center">
        <button
          on:click={() => likeExperiment(experiment.experiment.id)}
          class="{likedExperiment && likedExperiment == experiment.experiment.id
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
