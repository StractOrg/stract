<script lang="ts">
  import type { DisplayedSidebar } from '$lib/api';
  import Entity from './Entity.svelte';
  import StackOverflow from './StackOverflow.svelte';

  export let sidebar: DisplayedSidebar;
</script>

{#if sidebar._type == 'entity'}
  <Entity entity={sidebar.value} />
{:else if sidebar._type == 'stackOverflow'}
  <StackOverflow {...sidebar.value} />
{/if}
