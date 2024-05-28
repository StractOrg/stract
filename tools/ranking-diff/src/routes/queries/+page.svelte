<script lang="ts">
  import { onMount } from 'svelte';
  import AddIcon from '~icons/heroicons/plus-circle';
  import DeleteIcon from '~icons/heroicons/trash';
  import type { Query } from '$lib';
  import Navbar from '../components/Navbar.svelte';

  const addQuery = async (query: string) => {
    await fetch('/api/queries/insert', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
  };

  const addQuerySubmit = async (e: Event) => {
    e.preventDefault();

    if (!input) {
      return;
    }

    await addQuery(input.value);
    await getQueries();

    input.value = '';
  };

  const getQueries = async () => {
    const res = await fetch('/api/queries');
    queries = await res.json();
  };

  const clearQueries = async () => {
    await fetch('/api/queries/clear', { method: 'POST' });
    await getQueries();
  };

  const deleteQuery = async (id: number) => {
    await fetch('/api/queries/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id }),
    });
    await getQueries();
  };

  let queries: Query[] = [];

  let input: HTMLInputElement | null = null;
  onMount(async () => {
    await getQueries();
  });
</script>

<Navbar page="queries" />

<div class="flex w-full flex-col items-center">
  <h1 class="text-2xl">Queries</h1>

  <form class="flex gap-x-1" on:submit={addQuerySubmit}>
    <input
      bind:this={input}
      class="rounded border border-slate-400 px-2 py-1"
      placeholder="query"
      type="text"
    />
    <button type="submit"><AddIcon class="text-green-600" /></button>
  </form>

  <div class="mt-5 flex flex-col space-y-5">
    {#each queries as query}
      <div class="flex items-center gap-x-2">
        <button on:click={() => deleteQuery(query.id)} class="hover:text-red-600">
          <DeleteIcon class="w-3" />
        </button>
        <p class="w-full text-center">{query.text}</p>
      </div>
    {/each}
  </div>

  {#if queries.length == 0}
    <p class="text-gray-500">No queries available.</p>
  {:else}
    <button
      on:click={clearQueries}
      class="mt-10 h-8 w-40 rounded bg-red-500 text-white disabled:bg-gray-400">Clear All</button
    >
  {/if}
</div>
