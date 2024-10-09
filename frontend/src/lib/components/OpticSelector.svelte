<script lang="ts">
  import { DEFAULT_OPTICS, opticKey } from '$lib/optics';
  import { opticsStore, opticsShowStore } from '$lib/stores';
  import Select from './Select.svelte';
  import { derived } from 'svelte/store';

  export let searchOnChange: boolean;
  export let selected = '';

  for (const optic of DEFAULT_OPTICS) {
    if ($opticsShowStore[opticKey(optic)] !== undefined) continue;

    opticsShowStore.update(($opticsShow) => ({
      ...$opticsShow,
      [opticKey(optic)]: optic.shown,
    }));
  }

  const optics = derived(opticsStore, ($optics) => [...$optics, ...DEFAULT_OPTICS]);

  $: options = [
    { value: '', label: 'No Optic' },
    ...$optics
      .filter((optic) => $opticsShowStore[opticKey(optic)])
      .map((optic) => ({ value: optic.url, label: optic.name })),
  ];
</script>

<div class="noscript:hidden m-0 flex h-full flex-col justify-center p-0">
  <Select
    form="searchbar-form"
    id="optics-selector"
    name="optic"
    class="m-0 cursor-pointer text-xs text-neutral-focus"
    submitOnChange={searchOnChange}
    bind:value={selected}
    {options}
    ariaLabel="Optic selector"
  />
</div>
