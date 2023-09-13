<script lang="ts">
  import type { Region } from '$lib/api';
  import { ALL_REGIONS } from '$lib/regions';
  import Select from './Select.svelte';

  export let searchOnChange: boolean;
  export let selected: Region = ALL_REGIONS[0];

  $: options = ALL_REGIONS.map((region) => ({
    value: region,
    label: region == 'All' ? 'All Languages' : region,
  }));
</script>

<div class="m-0 flex h-full flex-col justify-center p-0">
  <Select
    form="searchbar-form"
    id="region-selector"
    name="gl"
    className="m-0 font-light text-neutral-focus"
    submitOnChange={searchOnChange}
    bind:value={selected}
    {options}
  />
</div>
