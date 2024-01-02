<script lang="ts">
  import type { Writable } from "svelte/store";

    const options = [
        ['on', true],
        ['off', false],
    ] as const;

    export let store: Writable<boolean>;
    export let prefix: string;
</script>
<div class="flex space-x-4 pr-2">
    {#each options as [name, state]}
      <label
        class="flex cursor-pointer flex-col items-center"
        for="{prefix}-{state}"
      >
        <input
          type="radio"
          name="{prefix}"
          id="{prefix}-{state}"
          value={state}
          bind:group={$store}
        />
        <span class="capitalize">{name}</span>
      </label>
    {/each}
  </div>
