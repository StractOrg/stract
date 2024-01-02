<script lang="ts">
  import Select from '$lib/components/Select.svelte';
  import { postSearchStore } from '$lib/stores';

  let options = [
    { value: void 0, label: 'GET' },
    { value: true, label: 'POST' },
  ];
</script>

<div class="noscript:hidden">
  <Select bind:value={$postSearchStore} class="text-neutral-focus" {options} />
</div>
