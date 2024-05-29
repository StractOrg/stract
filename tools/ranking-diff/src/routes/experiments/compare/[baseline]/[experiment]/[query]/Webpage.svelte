<script lang="ts">
  import type { SimpleWebpage } from '$lib/webpage';
  import TopSignals from './TopSignals.svelte';

  export let webpage: SimpleWebpage;
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
  <TopSignals signals={webpage.rankingSignals} />
</div>
