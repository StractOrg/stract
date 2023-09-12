<script lang="ts">
  import type { Region } from '$lib/api';
  import { ALL_REGIONS } from '$lib/regions';
  import Select from './Select.svelte';

  export let searchOnChange: boolean;
  export let selected: Region = ALL_REGIONS[0];
</script>

<div class="m-0 flex h-full flex-col justify-center p-0">
  <Select
    form="searchbar-form"
    id="region-selector"
    name="gl"
    className="m-0 font-light text-neutral-focus"
    submitOnChange={searchOnChange}
    bind:value={selected}
  >
    {#each ALL_REGIONS as region (region)}
      <option value={region} title={region} selected={region == selected}>
        {region == 'All' ? 'All Languages' : region}
      </option>
    {/each}
  </Select>
</div>
