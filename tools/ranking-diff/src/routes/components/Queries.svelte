<script lang="ts">
  import type { Query } from '$lib';

  export let queries: Query[];
</script>

<div class="flex w-full flex-col items-center">
  <h1 class="text-2xl">Queries</h1>

  {#each queries as query}
    <p>{query.text}</p>
  {/each}
</div>
