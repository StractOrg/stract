<script lang="ts">
  import SettingsIcon from '~icons/heroicons/cog';
  import { shuffleExperimentsStore, showSignalsStore } from '$lib/stores';

  let open = false;
</script>

<div>
  <button on:click={() => (open = !open)}>
    <SettingsIcon class="h-6 w-6" />
  </button>
  {#if open}
    <div>
      <div class="flex gap-x-1">
        <input id="shuffle_experiments" type="checkbox" bind:checked={$shuffleExperimentsStore} />
        <label for="shuffle_experiments">Shuffle Experiments</label>
      </div>
      <div class="flex gap-x-1">
        <input id="show_signals" type="checkbox" bind:checked={$showSignalsStore} />
        <label for="show_signals">Show Signals</label>
      </div>
    </div>
  {/if}
</div>
