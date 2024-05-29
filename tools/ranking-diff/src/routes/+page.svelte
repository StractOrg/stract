<script lang="ts">
  import type { Experiment, Query } from '$lib';
  import { onMount } from 'svelte';
  import Navbar from './components/Navbar.svelte';
  import AddIcon from '~icons/heroicons/plus-circle';
  import ExperimentComponent from './Experiment.svelte';
  import ExperimentProgress from './ExperimentProgress.svelte';

  const getExperiments = async () => {
    const res = await fetch('/api/experiments');
    experiments = await res.json();
  };

  const getQueries = async () => {
    const res = await fetch('/api/queries');
    queries = await res.json();
  };

  const newExperiment = async () => {
    if (queries.length == 0) {
      alert('No queries available. Please add a query first.');
      return;
    }

    const experiment = (await fetch('/api/experiments/new', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    }).then((res) => res.json())) as Experiment;

    currentlyBuildingExperiment = experiment;
  };

  const clearExperiments = async () => {
    await fetch('/api/experiments/clear', { method: 'POST' });
    await getExperiments();
  };

  const finishExperiment = async () => {
    currentlyBuildingExperiment = null;
    // reload page
    window.location.reload();
  };

  onMount(async () => {
    await getExperiments();
    await getQueries();
  });

  const selectExperiment = (id: number): boolean => {
    if (selectedExperimentIds.length >= 2) {
      return false;
    }

    selectedExperimentIds = [...selectedExperimentIds, id];

    selectedExperimentIds.sort((a, b) => {
      const aExp = experiments.find((e) => e.id === a)!;
      const bExp = experiments.find((e) => e.id === b)!;

      return aExp.name.localeCompare(bExp.name);
    });

    return true;
  };

  const deselectExperiment = (id: number) => {
    selectedExperimentIds = selectedExperimentIds.filter((i) => i !== id);
  };

  let experiments: Experiment[] = [];
  let queries: Query[] = [];

  $: selectedExperimentIds = [] as number[];

  let currentlyBuildingExperiment: Experiment | null = null;
</script>

<Navbar page="experiments" />

<div class="flex w-full flex-col items-center">
  <div class="flex">
    <h1 class="text-2xl">Experiments</h1>
    <button on:click={newExperiment} class="ml-2 text-green-500">
      <AddIcon class="h-6 w-6" />
    </button>
  </div>

  <div class="mt-5 flex flex-col space-y-5">
    {#if currentlyBuildingExperiment}
      <ExperimentProgress
        experiment={currentlyBuildingExperiment}
        {queries}
        on:finish={finishExperiment}
      />
    {/if}

    {#each experiments as experiment}
      <ExperimentComponent
        {experiment}
        on:delete={getExperiments}
        selectionCallback={selectExperiment}
        deselectionCallback={deselectExperiment}
      />
    {/each}

    {#if experiments.length == 0}
      <p class="text-gray-500">No experiments available.</p>
    {/if}
  </div>

  {#if experiments.length > 0}
    <div>
      <button
        on:click={clearExperiments}
        class="mt-10 h-8 w-40 rounded bg-red-500 text-white disabled:bg-gray-400"
      >
        Clear All
      </button>

      <button
        class="mt-10 h-8 w-40 rounded bg-sky-400 text-white disabled:bg-gray-400"
        disabled={selectedExperimentIds.length !== 2}
        on:click={() => {
          window.location.href = `/experiments/compare/${selectedExperimentIds[0]}/${selectedExperimentIds[1]}`;
        }}
      >
        Compare
      </button>
    </div>
  {/if}
</div>
