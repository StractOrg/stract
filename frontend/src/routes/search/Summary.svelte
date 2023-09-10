<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  export let inProgress: boolean;
  export let data: string;

  const dispatch = createEventDispatcher<{ hide: null }>();
</script>

<div class="rounded-lg bg-slate-100 p-4 dark:bg-stone-800">
  <div class="mb-2 flex justify-between font-light">
    <span>Summary</span>
    <button
      class="rounded bg-slate-200 px-2 text-xs text-slate-700 dark:bg-stone-700 dark:text-stone-100"
      on:click={() => dispatch('hide')}
    >
      hide summary
    </button>
  </div>
  <p class="line-clamp-3">
    {#if data}
      {data}
      {#if inProgress}
        <span class="inline-block -translate-y-0.5 animate-blink font-thin">|</span>
      {/if}
    {:else}
      <span class="flex">
        <span class="inline-block animate-bounce [animation-delay:000ms]">.</span>
        <span class="inline-block animate-bounce [animation-delay:100ms]">.</span>
        <span class="inline-block animate-bounce [animation-delay:200ms]">.</span>
      </span>
    {/if}
  </p>
</div>
