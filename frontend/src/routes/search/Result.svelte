<script lang="ts">
  import AdjustVertical from '~icons/heroicons/adjustments-vertical';
  import type { Webpage, TextSnippet } from '$lib/api';
  import { createEventDispatcher } from 'svelte';
  import { clearSummary, summariesStore } from '$lib/stores';
  import Summary from './Summary.svelte';
  import { derived } from 'svelte/store';
  import { improvements } from '$lib/improvements';

  export let webpage: Webpage;
  export let resultIndex: number;

  const summary = derived(summariesStore, ($summaries) => $summaries[webpage.url]);

  let button: HTMLButtonElement;

  const dispatch = createEventDispatcher<{ modal: HTMLButtonElement }>();
</script>

<div class="flex min-w-0 grow flex-col space-y-1">
  <div class="flex min-w-0">
    <div class="flex min-w-0 grow flex-col space-y-1">
      <div class="flex items-center text-sm">
        <a
          class="max-w-[calc(100%-100px)] truncate text-gray-800 dark:text-brand-100"
          href={webpage.url}
          use:improvements={resultIndex}
        >
          {webpage.prettyUrl}
        </a>
      </div>
      <a
        class="max-w-[calc(100%-30px)] truncate text-xl font-medium text-blue-800 visited:text-purple-800 hover:underline dark:text-blue-500 dark:visited:text-purple-500"
        title={webpage.title}
        href={webpage.url}
        use:improvements={resultIndex}
      >
        {webpage.title}
      </a>
    </div>
    <button
      class="noscript:hidden hidden w-5 min-w-fit items-center justify-center bg-transparent text-gray-700/50 hover:cursor-pointer hover:text-gray-700 dark:text-stone-400 dark:hover:text-stone-300 md:flex"
      bind:this={button}
      on:click|stopPropagation={() => dispatch('modal', button)}
    >
      <AdjustVertical class="text-xl" />
    </button>
  </div>
  <div class="text-sm font-normal text-snippet dark:text-stone-400 [&>b]:font-bold">
    {#if $summary}
      <Summary {...$summary} on:hide={() => clearSummary(webpage)} />
    {:else if webpage.snippet.type == 'normal'}
      <div class="snippet">
        <div class="line-clamp-3">
          <div class="inline">
            <span id="snippet-text" class="snippet-text [&:nth-child(2)]:before:content-['—']">
              {webpage.snippet.date || ''}
              {#each webpage.snippet.text.fragments as fragment}
                {#if fragment.kind == "normal"}
                  {fragment.text}
                {:else if fragment.kind == "highlighted"}
                  <b>{fragment.text}</b>
                {/if}
              {/each}
            </span>
          </div>
        </div>
      </div>
    {/if}
  </div>
</div>
