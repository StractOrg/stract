<script lang="ts">
  import ChevronLeft from '~icons/heroicons/chevron-left-20-solid';
  import ChevronRight from '~icons/heroicons/chevron-right-20-solid';
  import OpticSelector from '$lib/components/OpticSelector.svelte';
  import Searchbar from '$lib/components/Searchbar.svelte';
  import type { PageData } from './$types';
  import RegionSelect from '$lib/components/RegionSelect.svelte';
  import type { DisplayedWebpage } from '$lib/api';
  import { onMount } from 'svelte';
  import { searchQueryStore } from '$lib/stores';
  import { flip } from 'svelte/animate';
  import Result from './Result.svelte';
  import Sidebar from './Sidebar.svelte';
  import Widget from './Widget.svelte';
  import DirectAnswer from './DirectAnswer.svelte';
  import Discussions from './Discussions.svelte';
  import { page } from '$app/stores';
  import { updateQueryId } from '$lib/improvements';
  import { browser } from '$app/environment';
  import Modal from './Modal.svelte';

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
        top: window.scrollY + rect.top + rect.height/2,
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

</script>

{#if modal}
  <Modal {query} {modal} />
{/if}

{#if results.type == 'websites'}
  <div
    class="m-0 grid w-full gap-y-5 px-5 pt-4 md:grid-cols-[minmax(50ch,48rem)_1fr] grid-rows-[auto_1fr] md:gap-x-12 md:pl-20 lg:pl-28"
    style="text-rendering:optimizeLegibility;font-smooth:always;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;"
  >
    <div class="flex max-w-2xl flex-col space-y-1">
      <div class="w-full">
        <Searchbar {query} />
      </div>
      <div class="mx-auto flex w-full justify-end sm:justify-between">
        <div class="hidden sm:flex h-full flex-col space-x-2 text-xs text-neutral">
          <p class="h-fit">
            {#if results.numHits != null}
              Found <span class="font-medium">{results.numHits.toLocaleString()}</span> results in <span class="font-medium">{(
                (results.searchDurationMs ?? 0) / 1000
              ).toFixed(2)}s</span>
            {:else}
              Search took <span class="font-medium">{((results.searchDurationMs ?? 0) / 1000).toFixed(2)}s</span>
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
    <div class="col-start-1 flex min-w-0 max-w-2xl flex-col space-y-5">
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
        class="row-start-2 mx-auto max-w-[90vw] md:max-w-[30vw] justify-center md:col-start-2 md:row-span-2 md:row-start-1 md:pt-5"
      >
        <Sidebar sidebar={results.sidebar} />
      </div>
    {/if}
  </div>
{/if}
