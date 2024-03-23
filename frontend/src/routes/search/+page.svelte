<script lang="ts">
  import OpticSelector from '$lib/components/OpticSelector.svelte';
  import Searchbar from '$lib/components/Searchbar.svelte';
  import type { PageData } from './$types';
  import RegionSelect from '$lib/components/RegionSelect.svelte';
  import { searchQueryStore, useKeyboardShortcuts } from '$lib/stores';
  import { page } from '$app/stores';
  import { updateQueryId } from '$lib/improvements';
  import { browser } from '$app/environment';
  import Serp from './Serp.svelte';
  import { search } from '$lib/search';

  export let data: PageData;
  $: results = data.results;
  $: query = data.params.query;

  const keybind = new Keybind([
    { key: Keys.J, callback: searchCb.focusNextResult },
    { key: Keys.ARROW_DOWN, callback: searchCb.focusNextResult },
    { key: Keys.K, callback: searchCb.focusPrevResult },
    { key: Keys.ARROW_UP, callback: searchCb.focusPrevResult },
    { key: Keys.H, callback: searchCb.focusSearchBar },
    { key: Keys.FORWARD_SLASH, callback: searchCb.focusSearchBar },
    { key: Keys.V, callback: searchCb.openResultInNewTab },
    { key: Keys.SINGLE_QUOTE, callback: searchCb.openResultInNewTab },
    { key: Keys.T, callback: searchCb.scrollToTop },
    { key: Keys.D, callback: searchCb.domainSearch },
    { key: Keys.L, callback: searchCb.openResult },
    { key: Keys.O, callback: searchCb.openResult },
    { key: Keys.M, callback: searchCb.focusMainResult },
    { key: Keys.S, callback: searchCb.goToMisspellLink },
  ]);

  const onKeyDown = (event: KeyboardEvent) => {
    // Only call onKeyDown if the target is not an input element (ie. search bar)
    if (!((event.target as HTMLElement).nodeName === 'INPUT')) {
      keybind.onKeyDown(event, $useKeyboardShortcuts);
    }
  };

  let prevPageSearchParams: URLSearchParams | null = null;
  let nextPageSearchParams: URLSearchParams | null = null;

  $: {
    if (data.params.currentPage > 1) {
      const newParams = new URLSearchParams($page.url.searchParams);
      newParams.set('p', (data.params.currentPage - 1).toString());
      prevPageSearchParams = newParams;
    } else {
      prevPageSearchParams = null;
    }

    if (results && results.type == 'websites' && results.hasMoreResults) {
      const newParams = new URLSearchParams($page.url.searchParams);
      newParams.set('p', (data.params.currentPage + 1).toString());
      nextPageSearchParams = newParams;
    } else {
      nextPageSearchParams = null;
    }
  }

  const clientSearch = async () => {
    if (!browser) return;

    const res = await search(data.params, { fetch: fetch });

    if (res.type == 'bang') {
      window.location.replace(res.redirectTo);
      return null;
    }

    results = res;

    return res;
  };

  // NOTE: save the search query to be used in the navbar
  $: searchQueryStore?.set($page.url.search);
  let paramsForRedirect = new URLSearchParams($page.url.search);
  let serverSearch = paramsForRedirect.get('ssr') === 'true';
  paramsForRedirect.set('ssr', 'true');
  let encodedQueryForRedirect = paramsForRedirect.toString();

  $: {
    if (browser && results && results.type == 'websites')
      updateQueryId({ query, webpages: results.webpages });
  }
</script>

{#if !serverSearch}
  <noscript>
    <meta http-equiv="refresh" content="0;url=/search?{encodedQueryForRedirect}" />
    <div>
      You are being redirected to <a href="/search?{encodedQueryForRedirect}" class="underline"
        >a page that doesn't require javascript.</a
      >
    </div>
  </noscript>
{/if}

<div
  class="m-0 grid w-full grid-rows-[auto_1fr] gap-y-5 px-5 pt-4 md:grid-cols-[minmax(50ch,48rem)_1fr] md:gap-x-12 md:pl-20 lg:pl-28"
  style="text-rendering:optimizeLegibility;font-smooth:always;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;"
>
  <div class="flex max-w-2xl flex-col space-y-1">
    <div class="w-full">
      <Searchbar {query} />
    </div>
    <div class="mx-auto flex w-full justify-end sm:justify-between">
      <div class="hidden h-full flex-col space-x-2 text-xs text-neutral sm:flex">
        <p class="h-fit">
          {#if results && results.numHits != null}
            Found <span class="font-medium">{results.numHits.toLocaleString()}</span> results in
            <span class="font-medium">{((results.searchDurationMs ?? 0) / 1000).toFixed(2)}s</span>
          {:else if results}
            Search took <span class="font-medium"
              >{((results.searchDurationMs ?? 0) / 1000).toFixed(2)}s</span
            >
          {/if}
        </p>
      </div>
      <div class="flex space-x-2">
        <div class="m-0 flex h-full flex-col justify-center p-0">
          <OpticSelector searchOnChange={true} selected={data.params.optic} />
        </div>
        <div class="select-region flex h-full flex-col justify-center">
          <RegionSelect searchOnChange={true} selected={data.params.selectedRegion} />
        </div>
      </div>
    </div>
  </div>
  {#if results}
    <Serp
      {results}
      {query}
      {prevPageSearchParams}
      {nextPageSearchParams}
      currentPage={data.params.currentPage}
    />
  {:else}
    {#await clientSearch() then results}
      {#if results}
        <Serp
          {results}
          {query}
          {prevPageSearchParams}
          {nextPageSearchParams}
          currentPage={data.params.currentPage}
        />
      {/if}
    {/await}
  {/if}
</div>
