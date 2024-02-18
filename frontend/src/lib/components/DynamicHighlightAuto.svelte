<script lang="ts">
  import { onMount } from 'svelte';
  import type { HighlightAuto } from 'svelte-highlight';

  export let code: string;

  let component: typeof HighlightAuto;
  onMount(async () => {
    component = (await import('svelte-highlight')).HighlightAuto;
  });
</script>

<svelte:component this={component} {code} />
