<script lang="ts">
  import MagnifyingGlass from '~icons/heroicons/magnifying-glass';
  import Button from '$lib/components/Button.svelte';
  import { api } from '$lib/api';
  import { safeSearchStore, siteRankingsStore } from '$lib/stores';
  import { browser } from '$app/environment';
  import { derived } from 'svelte/store';
  import { compressRanked, rankingsToRanked } from '$lib/rankings';
  import { twJoin } from 'tailwind-merge';
  import { P, match } from 'ts-pattern';

  export let autofocus = false;

  export let query = '';
  let selected: 'none' | number = 'none';
  let suggestions: string[] = [];

  let cancelLastRequest: null | (() => void) = null;
  const updateSuggestions = (query: string) => {
    cancelLastRequest?.();

    if (!query) {
      suggestions = [];
      return;
    }

    const { data, cancel } = api.autosuggest({ q: query });
    cancelLastRequest = cancel;
    data.then((res) => (suggestions = res.map((x) => x.raw)));
  };

  let didChangeInput = false;
  let lastRealQuery = query;

  $: if (didChangeInput) lastRealQuery = query;
  $: if (browser) updateSuggestions(lastRealQuery);

  const compressedRanked = derived(siteRankingsStore, ($siteRankings) =>
    compressRanked(rankingsToRanked($siteRankings)),
  );

  const selectSuggestion = (s: string) => (query = s);

  const moveSelection = (step: number) => {
    selected = match(selected)
      .returnType<'none' | number>()
      .with(P.string, () => (step > 0 ? 0 : suggestions.length - 1))
      .with(
        P.when((v) => !(0 <= v + step && v + step < suggestions.length)),
        () => 'none',
      )
      .otherwise((v) => (v + suggestions.length + step) % suggestions.length);
    query = typeof selected == 'number' ? suggestions[selected] : (query = lastRealQuery);
    didChangeInput = false;
  };

  const onKeydown = (ev: KeyboardEvent) => {
    match(ev.key)
      .with('ArrowUp', () => {
        ev.preventDefault();
        moveSelection(-1);
      })
      .with('ArrowDown', () => {
        ev.preventDefault();
        moveSelection(1);
      })
      .otherwise(() => {
        didChangeInput = true;
      });
  };
</script>

<form action="/search" class="flex w-full justify-center" id="searchbar-form">
  <input type="hidden" value={$safeSearchStore ? 'true' : 'false'} name="ss" />
  <input type="hidden" value={$compressedRanked} name="sr" id="siteRankingsUuid" />

  <label
    for="searchbar"
    class={twJoin(
      'group relative grid w-full grid-cols-[auto_1fr_auto] items-center rounded-3xl border py-0.5 pl-3 pr-0.5 transition focus-within:shadow dark:border-stone-700 dark:bg-stone-800 focus-within:dark:border-stone-600',
      suggestions.length > 0 && 'focus-within:rounded-b-none',
    )}
  >
    <MagnifyingGlass class="w-5" />
    <!-- svelte-ignore a11y-autofocus -->
    <input
      type="search"
      id="searchbar"
      name="q"
      {autofocus}
      placeholder="Search"
      autocomplete="off"
      class="border-none bg-transparent focus:ring-0"
      bind:value={query}
      on:keydown={onKeydown}
    />
    <Button type="submit">search</Button>

    {#if suggestions.length > 0}
      <div
        class="absolute inset-x-5 bottom-px hidden h-px bg-gray-200 group-focus-within:block dark:bg-stone-700"
      />
      <div
        class="absolute -inset-x-px bottom-0 hidden translate-y-full flex-col overflow-hidden rounded-3xl rounded-t-none border border-t-0 bg-white shadow group-focus-within:flex dark:border-stone-700 group-focus-within:dark:border-stone-600"
      >
        {#each suggestions as s, index}
          <button
            class="flex space-x-3 py-1.5 pl-5 hover:bg-gray-50 dark:bg-stone-800 dark:hover:bg-stone-900 {selected ==
            index
              ? 'bg-gray-50 dark:hover:bg-stone-900'
              : ''}"
            on:click={() => selectSuggestion(s)}
            type="submit"
          >
            <MagnifyingGlass class="w-4" />
            <span>{s}</span>
          </button>
        {/each}
      </div>
    {/if}
  </label>
</form>
