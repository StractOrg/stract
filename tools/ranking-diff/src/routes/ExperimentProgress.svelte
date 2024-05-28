<script lang="ts">
  import type { Experiment, Query } from '$lib';
  import { createEventDispatcher, onMount } from 'svelte';
  import { tweened } from 'svelte/motion';

  export let experiment: Experiment;
  export let queries: Query[];

  // const API = 'http://localhost:3000/beta/api';
  const API = 'https://stract.com/beta/api';
  const REQ_DELAY_MS = 1000;

  let currentQuery = 0;

  const search = async (query: Query) => {
    const res = await fetch(`${API}/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: query.text }),
    });

    const data = await res.json();

    console.log(data.webpages);
  };

  onMount(async () => {
    for (let i = 0; i < queries.length; i++) {
      currentQuery = i;
      progress.set(((i + 1) / queries.length) * 100);

      await search(queries[i]);
      await new Promise((resolve) => setTimeout(resolve, REQ_DELAY_MS));
    }

    dispatch('finish');
  });

  const dispatch = createEventDispatcher();
  const progress = tweened(0, { duration: 100 });
</script>

<div>
  <span class="flex w-full gap-x-2">
    <p class="text-sm">{$progress.toFixed(0)}%</p>
    <progress max="100" value={$progress}></progress>
  </span>
  <p class="text-sm">processing: <i>{queries[currentQuery].text}</i>...</p>
</div>
