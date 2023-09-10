import { useComputed, useSignal, useSignalEffect } from "@preact/signals";
import {
  generateCombinedRankingsBase64,
  useAllRankings,
} from "../search/ranking.ts";
import { IS_BROWSER } from "$fresh/runtime.ts";
import * as search from "../search/index.ts";
import { match } from "ts-pattern";
import { HiMagnifyingGlass } from "../icons/HiMagnifyingGlass.tsx";
import { useSyncSignalWithLocalStorage } from "../search/utils.ts";
import { safeSearchSignal } from "../search/preferences.ts";
import { twMerge } from "tailwind-merge";
import { ComponentChild } from "preact";
import { Button } from "../components/Button.tsx";

export const Searchbar = (
  { autofocus = false, defaultQuery = "f" }: {
    autofocus?: boolean;
    defaultQuery?: string;
  },
) => {
  const query = useSignal(defaultQuery);
  const allRankings = useAllRankings();
  const compressed = IS_BROWSER
    ? useComputed(() =>
      generateCombinedRankingsBase64(
        allRankings.map(([sec, sig]) => [sec, sig.signal.value.data]),
      )
    )
    : "";

  useSyncSignalWithLocalStorage(safeSearchSignal);

  const suggestions = useSignal<search.Suggestion[]>([]);
  const selectedSignal = useSignal(0);

  useSignalEffect(() => {
    if (query.value == "") {
      suggestions.value = [];
      return;
    }

    const { data, cancel } = search.api.autosuggest({ q: query.value });

    data.then((res) => {
      suggestions.value = res;
    });

    return () => cancel();
  });

  const numEntries = suggestions.value.length + 1;

  const hoveredSuggestion = selectedSignal.value > 0 &&
      numEntries > selectedSignal.value
    ? suggestions.value[selectedSignal.value - 1]
    : null;

  return (
    <form
      class="relative w-full"
      id="searchbar-form"
      method="GET"
      action="/search"
      autocomplete="off"
    >
      <label
        id="searchbar"
        class={twMerge(`
            group relative z-40 top-0 grid grid-cols-[auto_1fr_auto] rounded-[1.25rem] py-0.5 pl-5 pr-0.5
            transition focus-within:shadow
            border border-gray-300 dark:border-stone-700 focus-within:dark:border-stone-600
            bg-white dark:bg-stone-800
            ${suggestions.value.length > 0 && "focus-within:rounded-b-none"}
          `)}
      >
        <HiMagnifyingGlass class="w-5 self-center text-gray-400" />
        <input
          type="text"
          value={hoveredSuggestion?.raw ?? query}
          autofocus={autofocus}
          name="q"
          default
          class="searchbar-input font-light peer relative inset-y-0 flex h-full w-full grow border-none bg-transparent py-0 outline-none focus:ring-0"
          placeholder="Search"
          onInput={(e) => {
            selectedSignal.value = 0;
            query.value = (e.target as HTMLInputElement).value;
          }}
          onKeyDown={(e) => {
            match(e.key)
              .with("ArrowUp", () => {
                e.preventDefault();
                selectedSignal.value = (selectedSignal.value + numEntries - 1) %
                  numEntries;
              })
              .with("ArrowDown", () => {
                e.preventDefault();
                selectedSignal.value = (selectedSignal.value + numEntries + 1) %
                  numEntries;
              })
              .with("Escape", () => {
                (e.target as HTMLInputElement).blur();
              })
              .otherwise(() => {});
          }}
        />
        <div class="flex items-center justify-center">
          <Button type="submit" title="Search">search</Button>
        </div>

        {suggestions.value.length > 0 && (
          <>
            <div class="hidden group-focus-within:block absolute inset-x-5 bottom-px h-px bg-gray-200 dark:bg-stone-700" />
            <div
              class={twMerge(`
              absolute hidden group-focus-within:flex bottom-px inset-x-0 flex-col translate-y-full overflow-hidden shadow rounded-b-[1.25rem] -mx-px
              border-x border-b border-gray-300 dark:border-stone-700 group-focus-within:dark:border-stone-600
              bg-white dark:bg-stone-800
            `)}
            >
              {suggestions.value.map((sug, idx) => (
                <button
                  class={twMerge(`
                    py-1.5 pl-5 flex cursor-pointer
                    hover:bg-gray-50 dark:hover:bg-stone-900
                    ${
                    selectedSignal.value == idx + 1
                      ? "bg-gray-50 dark:bg-stone-900"
                      : "bg-white dark:bg-stone-800"
                  }
                  `)}
                  onClick={(e) => {
                    selectedSignal.value = idx + 1;
                    (e.target as HTMLButtonElement).form!.submit();
                  }}
                >
                  <div class="flex w-4 mr-3 items-center">
                    <img class="h-5" loading="lazy" src="/images/search.svg" />
                  </div>
                  <div>
                    {Array.from({ length: sug.raw.length })
                      .reduce<[boolean, ComponentChild[]]>(
                        ([matching, acc], _, idx) =>
                          (!matching || sug.raw[idx] != query.value[idx])
                            ? [false, [
                              ...acc,
                              <span class="font-medium">{sug.raw[idx]}</span>,
                            ]]
                            : [true, [
                              ...acc,
                              <b class="font-light">{sug.raw[idx]}</b>,
                            ]],
                        [true, []],
                      )[1]}
                  </div>
                </button>
              ))}
            </div>
          </>
        )}
      </label>

      <input
        type="hidden"
        value={safeSearchSignal.value.data ? "true" : "false"}
        name="ss"
      />
      <input type="hidden" value={compressed} name="sr" id="siteRankingsUuid" />
    </form>
  );
};
