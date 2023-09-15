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
      .with('Enter', () => {
        hasFocus = false;
      })
      .otherwise(() => {
        didChangeInput = true;
      });
  };

  const splitAtOverlap = (suggestion: string) => {
    const lastIndex = [...suggestion].findLastIndex((a, i) => a == lastRealQuery[i]);
    return [suggestion.slice(0, lastIndex + 1), suggestion.slice(lastIndex + 1)];
  };

  let suggestionsDiv: HTMLDivElement | undefined;
  let hasFocus = autofocus;
</script>

<form action="/search" class="flex w-full justify-center" id="searchbar-form">
  <input type="hidden" value={$safeSearchStore ? 'true' : 'false'} name="ss" />
  <input type="hidden" value={$compressedRanked} name="sr" id="siteRankingsUuid" />

  <label
    for="searchbar"
    class={twJoin(
      'group relative grid w-full grid-cols-[auto_1fr_auto] items-center rounded-3xl border border-base-400 py-0.5 pl-5 pr-0.5 transition focus-within:shadow',
      hasFocus && suggestions.length > 0 && 'rounded-b-none',
      hasFocus && 'shadow',
    )}
  >
    <MagnifyingGlass class="w-5 text-base-content" />
    <!-- svelte-ignore a11y-autofocus -->
    <input
      type="search"
      id="searchbar"
      name="q"
      {autofocus}
      placeholder="Search"
      autocomplete="off"
      class="border-none bg-transparent focus:ring-0"
      on:focus={() => {
        hasFocus = true;
      }}
      on:blur={(e) => {
        // NOTE: If we click an element inside the suggestions,
        // don't blur yet since the clicked element would disapper
        if (e.relatedTarget instanceof Node && suggestionsDiv?.contains(e.relatedTarget)) return;

        requestIdleCallback(() => (hasFocus = false));
      }}
      bind:value={query}
      on:keydown={onKeydown}
    />
    <Button type="submit">search</Button>

    {#if suggestions.length > 0}
      <div class="absolute inset-x-5 bottom-px hidden h-px bg-base-300 group-focus-within:block" />
      <div
        class={twJoin(
          'absolute inset-x-5 bottom-px h-px bg-base-300',
          hasFocus ? 'block' : 'hidden',
        )}
      />
      <div
        class={twJoin(
          'absolute -inset-x-px bottom-0 translate-y-full flex-col overflow-hidden rounded-3xl rounded-t-none border border-t-0 border-base-400 bg-base-100 shadow',
          hasFocus ? 'flex' : 'hidden',
        )}
        role="listbox"
        bind:this={suggestionsDiv}
      >
        {#each suggestions as s, index}
          <button
            class={twJoin(
              'flex space-x-3 py-1.5 pl-5 hover:bg-base-200',
              selected == index && 'bg-base-200',
            )}
            on:click={() => {
              selectSuggestion(s);
              hasFocus = false;
            }}
            type="submit"
          >
            <MagnifyingGlass class="w-4 text-neutral" />
            <span>
              {@html splitAtOverlap(s)[0]}<span class="font-medium"
                >{@html splitAtOverlap(s)[1]}</span
              >
            </span></button
          >
        {/each}
      </div>
    {/if}
  </label>
</form>
