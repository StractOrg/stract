<script lang="ts">
  import { markPagesWithPaywallStore } from '$lib/stores';
  import RadioSelect from './RadioSelect.svelte';
</script>

<RadioSelect store={markPagesWithPaywallStore} prefix="mark-paywalls" />
