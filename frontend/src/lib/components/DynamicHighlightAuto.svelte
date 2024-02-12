<script lang="ts">
  import { onMount } from 'svelte';

  export let code: string;

  let component: any; // don't know what the type should be before import?
  onMount(async () => {
    component = (await import('svelte-highlight')).HighlightAuto;
  });
</script>

<svelte:component this={component} {code} />
