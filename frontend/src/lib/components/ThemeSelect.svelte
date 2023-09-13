<script lang="ts">
  import Select from './Select.svelte';
  import { themeStore } from '$lib/stores';
  import { THEMES } from '$lib/themes';

  $: options = [
    { value: void 0, label: 'Auto' },
    ...THEMES.map((t) => ({ value: t.class, label: t.name })),
  ];
</script>

<div class="noscript:hidden">
  <Select bind:value={$themeStore} class="text-neutral-focus" {options} />
</div>
