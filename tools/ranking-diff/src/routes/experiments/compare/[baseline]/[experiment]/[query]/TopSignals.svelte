<script lang="ts">
  export let signals: Record<string, number>;

  let showAll = false;
  const sortedSignals = Object.entries(signals).sort((a, b) => b[1] - a[1]);
</script>

<div class="flex flex-wrap gap-x-2 gap-y-1">
  {#each sortedSignals.slice(0, showAll ? undefined : 3) as [signal, value]}
    <span
      class="rounded-full border border-sky-300 bg-sky-100 px-1 text-xs"
      title={value.toFixed(2)}
      >{signal}
    </span>
  {/each}
  <button class="text-xs text-gray-400" on:click={() => (showAll = !showAll)}>
    {showAll ? 'Show less' : 'Show all'}
  </button>
</div>
