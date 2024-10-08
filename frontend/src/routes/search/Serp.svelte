<svelte:options accessors />

<script lang="ts">
  import { type DisplayedWebpage } from '$lib/api';
  import type { SearchResults } from '$lib/search';
  import { onMount } from 'svelte';

  import Modal from './Modal.svelte';
  import Result from './Result.svelte';
  import Widget from './Widget.svelte';
  import Discussions from './Discussions.svelte';
  import Sidebar from './Sidebar.svelte';

  import ChevronLeft from '~icons/heroicons/chevron-left-20-solid';
  import ChevronRight from '~icons/heroicons/chevron-right-20-solid';
  import { flip } from 'svelte/animate';
  import SpellCorrection from './SpellCorrection.svelte';

  export let results: SearchResults;
  export let query: string;
  export let nextPageSearchParams: URLSearchParams | null;
  export let prevPageSearchParams: URLSearchParams | null;
  export let currentPage: number;
  export let spellCorrectElem: SpellCorrection | undefined;
  export let resultElems: Result[];

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

  $: {
    results;
    modal = void 0;
  }
</script>

{#if modal}
  <Modal
    {modal}
    on:close={() => {
      modal = void 0;
    }}
  />
{/if}

{#if results._type == 'websites'}
  <div class="col-start-1 flex min-w-0 max-w-2xl flex-col space-y-5">
    {#if results.spellCorrection}
      <SpellCorrection spellCorrection={results.spellCorrection} bind:this={spellCorrectElem} />
    {/if}

    {#if results.widget}
      <Widget widget={results.widget} />
    {/if}

    {#if results.webpages}
      <div class="grid w-full grid-cols-1 space-y-10 place-self-start">
        {#each results.webpages as webpage, resultIndex (`${query}-${resultIndex}-${webpage.url}`)}
          <div animate:flip={{ duration: 150 }}>
            <Result
              bind:this={resultElems[resultIndex]}
              {webpage}
              {resultIndex}
              on:modal={openSearchModal(webpage)}
            />
          </div>
        {/each}
        {#if results.discussions}
          <Discussions discussions={results.discussions} />
        {/if}
      </div>
    {/if}

    <div class="flex justify-center">
      <div class="grid grid-cols-[repeat(3,auto)] items-center justify-center gap-2">
        {#if prevPageSearchParams}
          <a href="/search?{prevPageSearchParams}">
            <ChevronLeft class="text-xl text-primary hover:text-primary-focus" />
          </a>
        {:else}
          <ChevronLeft class="text-xl text-neutral" />
        {/if}
        <div>Page {currentPage}</div>
        {#if nextPageSearchParams}
          <a href="/search?{nextPageSearchParams}">
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
      class="row-start-2 mx-auto max-w-[90vw] justify-center md:col-start-2 md:row-span-2 md:row-start-1 md:max-w-[30vw] md:pt-5"
    >
      <Sidebar sidebar={results.sidebar} />
    </div>
  {/if}
{/if}
