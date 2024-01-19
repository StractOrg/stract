<script lang="ts">
  import XMark from '~icons/heroicons/x-mark-20-solid';
  import Callout from '$lib/components/Callout.svelte';
  import { createEventDispatcher } from 'svelte';
  import { summariesStore } from '$lib/stores';
  import { fade } from 'svelte/transition';

  export let url: string;

  $: tokens = ($summariesStore[url]?.tokens ?? []);

  const dispatch = createEventDispatcher<{ hide: null }>();
</script>

<Callout kind="neutral" title="Summary">
  <button slot="top-right" on:click={() => dispatch('hide')} title="Hide summary">
    <XMark />
  </button>

  <p class="line-clamp-3">
    {#if tokens.length > 0}
      {#each tokens as tok}
        <span transition:fade={{duration: 500}}>{tok}</span>
      {/each}
    {:else}
      <span class="flex">
        <span class="inline-block animate-bounce [animation-delay:000ms]">.</span>
        <span class="inline-block animate-bounce [animation-delay:100ms]">.</span>
        <span class="inline-block animate-bounce [animation-delay:200ms]">.</span>
      </span>
    {/if}
  </p>
</Callout>
