<script lang="ts">
  import { DEFAULT_OPTICS } from '$lib/optics';
  import { opticsStore } from '$lib/stores';
  import Select from './Select.svelte';
  import { derived } from 'svelte/store';

  export let searchOnChange: boolean;
  export let selected = '';

  const optics = derived(opticsStore, ($optics) => [...$optics, ...DEFAULT_OPTICS]);

  $: options = [
    { value: '', label: 'No Optic' },
    ...$optics.map((optic) => ({ value: optic.url, label: optic.name })),
  ];
</script>

<div class="m-0 flex h-full flex-col justify-center p-0">
  <Select
    form="searchbar-form"
    id="optics-selector"
    name="optic"
    class="m-0 font-light text-neutral-focus"
    submitOnChange={searchOnChange}
    bind:value={selected}
    {options}
  />
</div>
