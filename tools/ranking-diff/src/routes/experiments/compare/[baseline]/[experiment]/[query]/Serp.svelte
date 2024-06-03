<script lang="ts">
  import type { SimpleWebpage } from '$lib/webpage';
  import Webpage from './Webpage.svelte';

  export let webpages: SimpleWebpage[];
</script>

<div class="flex w-full flex-col gap-y-5">
  {#if webpages.length === 0}
    <i class="text-center">No webpages found</i>
  {:else}
    {#each webpages as webpage}
      <Webpage {webpage} />
    {/each}
  {/if}
</div>
