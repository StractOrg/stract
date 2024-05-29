<script lang="ts">
  import type { PageData } from './$types';
  import BackIcon from '~icons/heroicons/arrow-left';
  import Query from './Query.svelte';

  export let data: PageData;

  const { baseline, experiment, queries } = data;
</script>

<div>
  <a href="/">
    <BackIcon class="h-6 w-6" />
  </a>
</div>

<div class="flex flex-col gap-y-10">
  <h1 class="w-full text-center text-xl">
    Common queries between {baseline.name} and {experiment.name}
  </h1>
  <div class="grid grid-cols-5 gap-x-2">
    {#each queries as query}
      <Query {query} {experiment} {baseline} />
    {/each}
  </div>
</div>
