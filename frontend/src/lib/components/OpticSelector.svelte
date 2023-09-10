<script lang="ts">
  import { DEFAULT_OPTICS } from '$lib/optics';
  import { opticsStore } from '$lib/stores';
  import Select from './Select.svelte';
  import { derived } from 'svelte/store';

  export let searchOnChange: boolean;
  export let selected = '';

  const optics = derived(opticsStore, ($optics) => [...$optics, ...DEFAULT_OPTICS]);
</script>

<div class="m-0 flex h-full flex-col justify-center p-0">
  <Select
    form="searchbar-form"
    id="optics-selector"
    name="optic"
    className="m-0 font-light dark:text-stone-400"
    submitOnChange={searchOnChange}
    bind:value={selected}
  >
    <option value="">No Optic</option>
    {#each $optics as optic}
      <option value={optic.url} title={optic.description} selected={selected == optic.url}>
        {optic.name}
      </option>
    {/each}
  </Select>
</div>
