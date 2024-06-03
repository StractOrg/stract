<script lang="ts">
  import type { PageData } from './$types';
  import BackIcon from '~icons/heroicons/arrow-left';
  import Query from './Query.svelte';
  import Settings from '../../../../components/Settings.svelte';

  export let data: PageData;

  const { baseline, experiment, queries } = data;
</script>

<div class="flex w-full justify-between">
  <div class="flex w-fit gap-x-5">
    <a href="/">
      <BackIcon class="h-6 w-6" />
    </a>
    <div class="flex gap-2">
      <a href="/experiments/compare/{baseline.id}/{experiment.id}">
        <span class="rounded-full border border-slate-500 px-2 py-1">Queries</span>
      </a>
      <a href="/experiments/compare/{baseline.id}/{experiment.id}/categories">
        <span class="rounded-full border-slate-500 px-2 py-1">Categories</span>
      </a>
    </div>
  </div>
  <div>
    <Settings />
  </div>
</div>

<div class="flex flex-col py-10">
  <div class="grid grid-cols-5 gap-2">
    {#each queries as query}
      <Query {query} {experiment} {baseline} />
    {/each}
  </div>
</div>
