<script lang="ts">
  import { useKeyboardShortcuts } from '$lib/stores';
  import RadioSelect from './RadioSelect.svelte';
</script>

<RadioSelect store={useKeyboardShortcuts} prefix="use-keeb-shortcuts" />
