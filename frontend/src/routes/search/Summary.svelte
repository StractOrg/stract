<script lang="ts">
  import XMark from '~icons/heroicons/x-mark-20-solid';
  import Callout from '$lib/components/Callout.svelte';
  import { createEventDispatcher } from 'svelte';

  export let inProgress: boolean;
  export let data: string;

  const dispatch = createEventDispatcher<{ hide: null }>();
</script>

<Callout kind="neutral" title="Summary">
  <button slot="top-right" on:click={() => dispatch('hide')} title="Hide summary">
    <XMark />
  </button>

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
</Callout>
