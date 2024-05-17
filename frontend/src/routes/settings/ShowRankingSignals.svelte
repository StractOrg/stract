<script lang="ts">
  import { showRankingSignals } from '$lib/stores';
  import RadioSelect from './RadioSelect.svelte';
</script>

<RadioSelect store={showRankingSignals} prefix="show-ranking-signals" />
