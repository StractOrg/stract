<script lang="ts">
  import { safeSearchStore } from '$lib/stores';
  import RadioSelect from './RadioSelect.svelte';
</script>

<RadioSelect store={safeSearchStore} prefix="safe-search" />
