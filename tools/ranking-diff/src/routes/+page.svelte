<script lang="ts">
  import type { PageData } from './$types';
  import Experiments from './components/Experiments.svelte';
  import Queries from './components/Queries.svelte';
  export let data: PageData;

  type Page = 'experiments' | 'queries';

  let page: Page = 'experiments';

  const { experiments, queries } = data;
</script>

<nav class="flex gap-x-2">
  <button
    on:click|preventDefault={() => (page = 'experiments')}
    class="border-content rounded-full border-slate-500 px-2 py-1 {page === 'experiments'
      ? 'border'
      : ''}"
  >
    Experiments
  </button>
  <button
    on:click|preventDefault={() => (page = 'queries')}
    class="border-content rounded-full border-slate-500 px-2 py-1 {page === 'queries'
      ? 'border'
      : ''}"
  >
    Queries
  </button>
</nav>

{#if page === 'experiments'}
  <Experiments {experiments} />
{:else}
  <Queries {queries} />
{/if}
