<svelte:options accessors />

<script lang="ts">
  import AdjustVertical from '~icons/heroicons/adjustments-vertical';
  import type { DisplayedWebpage } from '$lib/api';
  import { createEventDispatcher } from 'svelte';
  import { markPagesWithAdsStore, markPagesWithPaywallStore } from '$lib/stores';
  import TextSnippet from '$lib/components/TextSnippet.svelte';
  import StackOverflowSnippet from './StackOverflowSnippet.svelte';
  import ResultLink from './ResultLink.svelte';
  import { hostRankingsStore } from '$lib/stores';
  import type { Ranking } from '$lib/rankings';
  import HandThumbDown from '~icons/heroicons/hand-thumb-down-20-solid';
  import HandThumbUp from '~icons/heroicons/hand-thumb-up-20-solid';

  export let webpage: DisplayedWebpage;
  export let resultIndex: number;

  let ranking: Ranking | undefined = undefined;
  hostRankingsStore.subscribe((rankings) => {
    if (rankings) {
      ranking = rankings[webpage.site];
    }
  });

  let button: HTMLButtonElement;

  const dispatch = createEventDispatcher<{ modal: HTMLButtonElement }>();

  let mainDiv: HTMLElement | undefined = undefined;
  export const getMainDiv = () => mainDiv;

  let mainResultLink: ResultLink | undefined = undefined;
  export const getMainResultLink = () => mainResultLink;

  export const hasFocus = () => mainResultLink?.hasFocus();
  export const clearFocus = () => mainResultLink?.clearFocus();
</script>

<span>
  <div class="flex min-w-0 grow flex-col space-y-0.5" bind:this={mainDiv}>
    <div class="flex min-w-0">
      <div class="flex min-w-0 grow flex-col space-y-0.5">
        <span class="flex flex-col-reverse">
          <h3 class="flex">
            <ResultLink
              _class="title line-clamp-2 md:line-clamp-1 max-w-[calc(100%-30px)] text-xl font-medium text-link visited:text-link-visited hover:underline"
              title={webpage.title}
              href={webpage.url}
              {resultIndex}
              bind:this={mainResultLink}
            >
              {webpage.title}
            </ResultLink>
          </h3>
          <div class="flex items-center text-sm">
            <ResultLink
              _class="url max-w-[calc(100%-100px)] truncate text-neutral-focus"
              href={webpage.url}
              {resultIndex}
            >
              {webpage.prettyUrl}
            </ResultLink>
          </div>
        </span>
      </div>
      <div class="flex space-x-2">
        {#if ranking}
          <span class="flex items-center text-sm text-neutral-focus">
            <span class="text-xs text-neutral">
              {#if ranking == 'liked'}
                <div title="liked site" aria-label="you have liked this site">
                  <HandThumbUp class="w-3 text-success" />
                </div>
              {:else if ranking == 'disliked'}
                <div aria-label="you have disliked this site" title="disliked site">
                  <HandThumbDown class="w-3 text-warning" />
                </div>
              {/if}
            </span>
          </span>
        {/if}
        <button
          class="noscript:hidden flex w-5 min-w-fit items-center justify-center bg-transparent text-neutral hover:cursor-pointer hover:text-neutral-focus"
          aria-label="Open modal for result number: {resultIndex}"
          bind:this={button}
          on:click|stopPropagation={() => dispatch('modal', button)}
        >
          <AdjustVertical class="text-md" aria-label="3 vertical bars" />
        </button>
      </div>
    </div>
    <p class="snippet text-sm font-normal text-neutral-focus [&>b]:font-bold">
      {#if webpage.richSnippet && webpage.richSnippet._type == 'stackOverflowQA'}
        <StackOverflowSnippet
          question={webpage.richSnippet.question}
          answers={webpage.richSnippet.answers}
        />
      {:else}
        <span class="line-clamp-4 md:line-clamp-3">
          <span class="inline">
            <span id="snippet-text" class="snippet-text">
              {#if webpage.likelyHasAds && $markPagesWithAdsStore && webpage.likelyHasPaywall && $markPagesWithPaywallStore}
                <span
                  class="rounded border border-primary p-0.5 text-center text-xs text-neutral"
                  title="page likely has ads and paywall"
                >
                  has ads + paywall
                </span>
              {:else if webpage.likelyHasAds && $markPagesWithAdsStore}
                <span
                  class="rounded border border-primary p-0.5 text-center text-xs text-neutral"
                  title="page likely has ads"
                >
                  has ads
                </span>
              {:else if webpage.likelyHasPaywall && $markPagesWithPaywallStore}
                <span
                  class="rounded border border-primary p-0.5 text-center text-xs text-neutral"
                  title="page likely has paywall"
                >
                  paywall
                </span>
              {/if}
              {#if webpage.snippet.date}
                <span class="text-neutral">
                  {webpage.snippet.date}
                </span> -
              {/if}
              <span>
                <TextSnippet snippet={webpage.snippet.text} />
              </span>
            </span>
          </span>
        </span>
      {/if}
    </p>
  </div>
</span>
