<svelte:options accessors />

<script lang="ts">
  import MagnifyingGlass from '~icons/heroicons/magnifying-glass';
  import Button from '$lib/components/Button.svelte';
  import { api, type HighlightedFragment } from '$lib/api';
  import { safeSearchStore, postSearchStore } from '$lib/stores';
  import { browser } from '$app/environment';
  import { twJoin } from 'tailwind-merge';
  import { P, match } from 'ts-pattern';

  export let autofocus = false;
  export let query = '';

  let selected: 'none' | number = 'none';
  let suggestions: HighlightedFragment[][] = [];

  let cancelLastRequest: null | (() => void) = null;

  const suggestionText = (s: HighlightedFragment[]): string => s.map((x) => x.text).join('');

  const updateSuggestions = (query: string) => {
    cancelLastRequest?.();

    if (!query) {
      suggestions = [];
      return;
    }

    const { data, cancel } = api.autosuggest({ q: query });
    cancelLastRequest = cancel;
    data.then((res) => (suggestions = res.map((x) => x.highlighted)));
  };

  let didChangeInput = false;
  let lastRealQuery = query;

  $: if (didChangeInput) lastRealQuery = query;
  $: if (browser) updateSuggestions(lastRealQuery);

  const selectSuggestion = (s: HighlightedFragment[]) => (query = suggestionText(s));

  const moveSelection = (step: number) => {
    selected = match(selected)
      .returnType<'none' | number>()
      .with(P.string, () => (step > 0 ? 0 : suggestions.length - 1))
      .with(
        P.when((v) => !(0 <= v + step && v + step < suggestions.length)),
        () => 'none',
      )
      .otherwise((v) => (v + suggestions.length + step) % suggestions.length);
    query = typeof selected == 'number' ? suggestionText(suggestions[selected]) : lastRealQuery;
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

  let suggestionsDiv: HTMLDivElement | undefined;
  let hasFocus = autofocus;

  let formElem: HTMLFormElement;
  let inputElem: HTMLInputElement;
  export const getInputElem = () => inputElem;
  export const getForm = () => formElem;
  export const select = () => inputElem.select();
  export const userQuery = () => lastRealQuery;
  export const search = (q: string) => {
    if (formElem && inputElem) {
      inputElem.value = q;
      formElem.submit();
    }
  };
</script>

<form
  action="/search"
  class="flex w-full justify-center"
  id="searchbar-form"
  method={$postSearchStore ? 'POST' : 'GET'}
  bind:this={formElem}
>
  <input type="hidden" value={$safeSearchStore ? 'true' : 'false'} name="ss" />

  <label
    for="searchbar"
    class={twJoin(
      'group relative grid w-full grid-cols-[auto_1fr_auto] items-center rounded-3xl border border-base-400 pl-5 transition focus-within:shadow',
      hasFocus && suggestions.length > 0 && 'rounded-b-none',
      hasFocus && 'shadow',
    )}
    aria-autocomplete="list"
    aria-expanded={suggestions.length > 0 && hasFocus}
  >
    <MagnifyingGlass class="w-5 text-base-content" />
    <!-- svelte-ignore a11y-autofocus -->
    <input
      id="searchbar"
      name="q"
      {autofocus}
      placeholder="Search"
      autocomplete="off"
      aria-expanded={suggestions.length > 0 && hasFocus}
      class="border-none bg-transparent text-lg focus:ring-0"
      on:focus={() => {
        hasFocus = true;
      }}
      on:blur={(e) => {
        // NOTE: If we click an element inside the suggestions,
        // don't blur yet since the clicked element would disapper
        if (e.relatedTarget instanceof Node && suggestionsDiv?.contains(e.relatedTarget)) return;

        // @ts-expect-error requestIdleCallback is not supported in Safari
        // https://caniuse.com/requestidlecallback
        if (window.requestIdleCallback) {
          requestIdleCallback(() => (hasFocus = false));
        } else {
          setTimeout(() => (hasFocus = false), 0);
        }
      }}
      bind:value={query}
      on:keydown={onKeydown}
      bind:this={inputElem}
    />
    <div class="h-full py-0.5 pr-0.5">
      <Button _class="py-0 h-full" type="submit">search</Button>
    </div>

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
              {#each s as fragment}
                {#if fragment.kind == 'highlighted'}
                  <span class="font-medium">{fragment.text}</span>
                {:else}
                  {fragment.text}
                {/if}
              {/each}
            </span></button
          >
        {/each}
      </div>
    {/if}
  </label>
  <noscript>
    <input type="hidden" value="true" name="ssr" />
  </noscript>
</form>
