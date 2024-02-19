<script lang="ts">
  import { markPagesWithAdsStore } from '$lib/stores';
  import RadioSelect from './RadioSelect.svelte';
</script>

<RadioSelect store={markPagesWithAdsStore} prefix="mark-ads" />
