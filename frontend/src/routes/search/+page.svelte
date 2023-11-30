<script lang="ts">
  import HandThumbDown from '~icons/heroicons/hand-thumb-down-20-solid';
  import HandThumbUp from '~icons/heroicons/hand-thumb-up-20-solid';
  import NoSymbol from '~icons/heroicons/no-symbol-20-solid';
  import ChevronLeft from '~icons/heroicons/chevron-left-20-solid';
  import ChevronRight from '~icons/heroicons/chevron-right-20-solid';
  import OpticSelector from '$lib/components/OpticSelector.svelte';
  import Searchbar from '$lib/components/Searchbar.svelte';
  import type { PageData } from './$types';
  import { scale } from 'svelte/transition';
  import RegionSelect from '$lib/components/RegionSelect.svelte';
  import type { DisplayedWebpage } from '$lib/api';
  import { onMount } from 'svelte';
  import { twJoin } from 'tailwind-merge';
  import Button from '$lib/components/Button.svelte';
  import { searchQueryStore, siteRankingsStore, summarize } from '$lib/stores';
  import type { Ranking } from '$lib/rankings';
  import { flip } from 'svelte/animate';
  import Result from './Result.svelte';
  import Sidebar from './Sidebar.svelte';
  import Widget from './Widget.svelte';
  import DirectAnswer from './DirectAnswer.svelte';
  import Discussions from './Discussions.svelte';
  import { page } from '$app/stores';
  import { updateQueryId } from '$lib/improvements';
  import { browser } from '$app/environment';

  export let data: PageData;
  $: results = data.results;
  $: query = data.query;

  let modal: { top: number; left: number; site: DisplayedWebpage } | undefined;

  onMount(() => {
    const listener = () => {
      modal = void 0;
    };
    document.addEventListener('click', listener);
    return () => document.removeEventListener('click', listener);
  });

  const openSearchModal =
    (site: DisplayedWebpage) =>
    ({ detail: button }: CustomEvent<HTMLButtonElement>) => {
      const rect = button.getBoundingClientRect();

      if (modal?.site == site) {
        modal = void 0;
        return;
      }

      // NOTE: The point calculated is the middle of the right edge of the clicked
      // element, like so:
      //     +---+
      //     |   x <--
      //     +---+
      modal = {
        top: window.scrollY + rect.top + rect.height / 2,
        left: window.scrollX + rect.right,
        site,
      };
    };

  // NOTE: save the search query to be used in the header
  $: searchQueryStore?.set($page.url.search);

  $: {
    results;
    modal = void 0;
  }

  // $: {
  //   results.discussions = results.webpages;
  // }

  $: {
    if (browser && results.type == 'websites') updateQueryId({ query, webpages: results.webpages });
  }

  const rankingChoices = [
    {
      ranking: 'liked',
      kind: 'success',
      Icon: HandThumbUp,
    },
    {
      ranking: 'disliked',
      kind: 'warning',
      Icon: HandThumbDown,
    },
    {
      ranking: 'blocked',
      kind: 'error',
      Icon: NoSymbol,
    },
  ] as const;

  const rankSite = (site: DisplayedWebpage, ranking: Ranking) => () => {
    siteRankingsStore?.update(($rankings) => ({
      ...$rankings,
      [site.domain]: $rankings[site.domain] == ranking ? void 0 : ranking,
    }));
  };

  const summarizeSite = (site: DisplayedWebpage) => () => summarize(query, site);
</script>

{#if modal}
  <!-- svelte-ignore a11y-click-events-have-key-events -->
  <!-- svelte-ignore a11y-no-static-element-interactions -->
  <div
    class={twJoin(
      'absolute -translate-y-1/2 transition-all',
      'h-fit w-52 flex-col items-center overflow-hidden rounded-lg border bg-base-100 px-2 py-5 text-sm drop-shadow-md',
    )}
    style="top: {modal.top}px; left: calc({modal.left}px + 1rem)"
    transition:scale={{ duration: 150 }}
    on:click|stopPropagation={() => {}}
  >
    <div>
      <h2 class="w-fit text-center">
        Do you like results from {modal.site.domain}?
      </h2>
      <div class="flex justify-center space-x-1.5 pt-2">
        {#each rankingChoices as { ranking, kind, Icon }}
          <Button
            {kind}
            pale={$siteRankingsStore[modal.site.domain] != ranking}
            padding={false}
            form="searchbar-form"
            on:click={rankSite(modal.site, ranking)}
          >
            <Icon class="w-4" />
          </Button>
        {/each}
      </div>
      <div class="mt-4 flex justify-center">
        <Button pale on:click={summarizeSite(modal.site)}>Summarize Result</Button>
      </div>
    </div>
  </div>
{/if}

{#if results.type == 'websites'}
  <div
    class="m-0 grid w-full gap-y-6 px-5 pt-4 md:grid-cols-[minmax(50ch,48rem)_1fr] md:grid-rows-[auto_1fr] md:gap-x-12 md:pl-20 lg:px-36"
    style="text-rendering:optimizeLegibility;font-smooth:always;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;"
  >
    <div class="flex max-w-2xl flex-col space-y-3">
      <div class="w-full">
        <Searchbar {query} />
      </div>
      <div class="mx-auto flex w-full justify-between">
        <div class="flex h-full flex-col justify-center space-x-2 text-sm text-neutral">
          <p class="h-fit">
            {#if results.numHits != null}
              Found {results.numHits.toLocaleString()} results in {(
                (results.searchDurationMs ?? 0) / 1000
              ).toFixed(2)}
              seconds
            {:else}
              Search took {((results.searchDurationMs ?? 0) / 1000).toFixed(2)} seconds
            {/if}
          </p>
        </div>
        <div class="flex space-x-2">
          <div class="m-0 flex h-full flex-col justify-center p-0">
            <OpticSelector searchOnChange={true} selected={data.optic} />
          </div>
          <div class="select-region flex h-full flex-col justify-center">
            <RegionSelect searchOnChange={true} selected={data.selectedRegion} />
          </div>
        </div>
      </div>
    </div>
    <div class="col-start-1 flex min-w-0 max-w-2xl flex-col space-y-10">
      {#if results.spellCorrectedQuery}
        <div>
          <div>
            Did you mean:{' '}
            <a
              class="font-medium"
              href="/search?q={encodeURIComponent(results.spellCorrectedQuery.raw)}"
              >{@html results.spellCorrectedQuery.highlighted}</a
            >
          </div>
        </div>
      {/if}

      {#if results.widget}
        <Widget widget={results.widget} />
      {/if}

      {#if results.directAnswer}
        <DirectAnswer directAnswer={results.directAnswer} />
      {/if}

      {#if results.webpages}
        <div class="grid grid-cols-1 space-y-10 place-self-start">
          {#each results.webpages as webpage, resultIndex (`${query}-${webpage.url}`)}
            <div animate:flip={{ duration: 150 }}>
              <Result {webpage} {resultIndex} on:modal={openSearchModal(webpage)} />
            </div>
          {/each}
          {#if results.discussions}
            <div class="row-start-5">
              <Discussions discussions={results.discussions} />
            </div>
          {/if}
        </div>
      {/if}

      <div class="flex justify-center">
        <div class="grid grid-cols-[repeat(3,auto)] items-center justify-center gap-2">
          {#if data.prevPageSearchParams}
            <a href="/search?{data.prevPageSearchParams}">
              <ChevronLeft class="text-xl text-primary hover:text-primary-focus" />
            </a>
          {:else}
            <ChevronLeft class="text-xl text-neutral" />
          {/if}
          <div>Page {data.currentPage}</div>
          {#if data.nextPageSearchParams}
            <a href="/search?{data.nextPageSearchParams}">
              <ChevronRight class="text-xl text-primary hover:text-primary-focus" />
            </a>
          {:else}
            <ChevronRight class="text-xl text-neutral" />
          {/if}
        </div>
      </div>
    </div>

    {#if results.sidebar}
      <div
        class="row-start-2 mx-auto max-w-[90vw] justify-center md:col-start-2 md:row-span-2 md:row-start-1 md:pt-10"
      >
        <Sidebar sidebar={results.sidebar} />
      </div>
    {/if}
  </div>
{/if}
