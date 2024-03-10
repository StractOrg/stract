<script lang="ts">
  import { resultsInNewTab } from '$lib/stores';
  import RadioSelect from './RadioSelect.svelte';
</script>

<RadioSelect store={resultsInNewTab} prefix="results-in-new-tabs" />
