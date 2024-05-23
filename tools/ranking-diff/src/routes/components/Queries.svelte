<script lang="ts">
  import AddIcon from '~icons/heroicons/plus-circle';
  import type { Query } from '$lib';

  const addQuery = (e: Event) => {
    e.preventDefault();

    if (!input) {
      return;
    }

    queries = [...queries, { id: 123, text: input.value }];
    input.value = '';
  };

  export let queries: Query[];

  let input: HTMLInputElement | null = null;
</script>

<div class="flex w-full flex-col items-center">
  <h1 class="text-2xl">Queries</h1>

  <form class="flex gap-x-1" on:submit={addQuery}>
    <input
      bind:this={input}
      class="rounded border border-slate-400 px-2 py-1"
      placeholder="query"
      type="text"
    />
    <button type="submit"><AddIcon class="text-green-600" /></button>
  </form>

  {#each queries as query}
    <p>{query.text}</p>
  {/each}
</div>
