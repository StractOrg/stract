<script lang="ts">
  import type { Experiment } from '$lib';
  import { onMount } from 'svelte';
  import Navbar from './components/Navbar.svelte';
  import AddIcon from '~icons/heroicons/plus-circle';
  import ExperimentComponent from './Experiment.svelte';

  const getExperiments = async () => {
    const res = await fetch('/api/experiments');
    experiments = await res.json();
  };

  const newExperiment = async () => {
    await fetch('/api/experiments/new', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });

    await getExperiments();
  };

  const clearExperiments = async () => {
    await fetch('/api/experiments/clear', { method: 'POST' });
    await getExperiments();
  };

  onMount(async () => {
    await getExperiments();
  });

  let experiments: Experiment[] = [];
</script>

<Navbar page="experiments" />

<div class="flex w-full flex-col items-center">
  <div class="flex">
    <h1 class="text-2xl">Experiments</h1>
    <button on:click={newExperiment} class="ml-2">
      <AddIcon class="h-6 w-6 text-green-500" />
    </button>
  </div>

  <div class="mt-5 flex flex-col space-y-5">
    {#each experiments as experiment}
      <ExperimentComponent {experiment} on:delete={getExperiments} />
    {/each}
  </div>

  <button
    on:click={clearExperiments}
    class="mt-10 h-8 w-40 rounded bg-red-500 text-white disabled:bg-gray-400"
    disabled={experiments.length == 0}>Clear All</button
  >
</div>
