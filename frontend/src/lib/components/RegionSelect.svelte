<script lang="ts">
  import { type Region, REGIONS } from '$lib/api';
  import Select from './Select.svelte';

  export let searchOnChange: boolean;
  export let selected: Region = REGIONS[0];

  $: options = REGIONS.map((region) => ({
    value: region,
    label: region == 'All' ? 'All Languages' : region,
  }));
</script>

<div class="m-0 flex h-full flex-col justify-center p-0">
  <Select
    form="searchbar-form"
    id="region-selector"
    name="gl"
    class="m-0 font-light text-neutral-focus text-sm"
    submitOnChange={searchOnChange}
    bind:value={selected}
    {options}
  />
</div>
