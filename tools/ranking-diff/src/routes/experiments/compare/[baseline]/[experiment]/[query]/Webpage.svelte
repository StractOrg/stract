<script lang="ts">
  import type { SimpleWebpage } from '$lib/webpage';
  import TopSignals from './TopSignals.svelte';
  import { showSignalsStore } from '$lib/stores';

  export let webpage: SimpleWebpage;

  const totalScore = Object.values(webpage.rankingSignals).reduce((acc, value) => acc + value, 0);
</script>

<div>
  <h2 class="line-clamp-1 font-medium" title={webpage.title}>{webpage.title}</h2>
  <a
    class="line-clamp-1 text-xs underline"
    href={webpage.url}
    title={webpage.url}
    target="_blank"
    rel="noopener noreferrer">{webpage.url}</a
  >
  <p class="line-clamp-3 text-sm">{webpage.snippet}</p>
  <p class="text-xs text-gray-500">Score: {totalScore.toFixed(4)}</p>
  {#if $showSignalsStore}
    <TopSignals signals={webpage.rankingSignals} />
  {/if}
</div>
