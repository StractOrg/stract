<script lang="ts">
  import type { Experiment } from '$lib';

  export let experiments: Experiment[];
</script>

<div class="flex w-full flex-col items-center">
  <h1 class="text-2xl">Experiments</h1>

  {#each experiments as experiment}
    <p>{experiment.name}</p>
  {/each}
</div>
