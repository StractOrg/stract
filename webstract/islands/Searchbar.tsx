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
import { tx } from "https://esm.sh/@twind/core@1.1.3";

export const Searchbar = (
  { autofocus = false, defaultQuery = "" }: {
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
  const selectedSignal = useSignal(-1);

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

  const hoveredSuggestion = selectedSignal.value >= 0 &&
      suggestions.value.length > selectedSignal.value
    ? suggestions.value[selectedSignal.value]
    : null;

  return (
    <form
      class="relative w-full"
      id="searchbar-form"
      method="GET"
      action="/search"
      autocomplete="off"
    >
      <div class="h-10">
        <div
          id="searchbar"
          class="group absolute z-40 inset-x-0 top-0 grid grid-cols-[auto_1fr_auto] grid-rows-[2.5rem] rounded-3xl border bg-white transition-shadow focus-within:shadow overflow-hidden"
        >
          <HiMagnifyingGlass class="col-[1/2] row-start-1 w-5 self-center ml-5 text-gray-400" />
          <input
            type="text"
            value={hoveredSuggestion?.raw ?? query}
            autofocus={autofocus}
            name="q"
            default
            class="searchbar-input font-light peer inset-y-0 col-[1/3] row-start-1 flex h-full w-full grow border-none bg-transparent py-0 pl-12 outline-none focus:ring-0"
            placeholder="Search"
            onInput={(e) => {
              selectedSignal.value = -1;
              query.value = (e.target as HTMLInputElement).value;
            }}
            onKeyDown={(e) => {
              match(e.key)
                .with("ArrowUp", () => {
                  e.preventDefault();
                  if (selectedSignal.value == -1) {
                    selectedSignal.value = suggestions.value.length - 1;
                  } else {
                    selectedSignal.value = (selectedSignal.value - 1) %
                      suggestions.value.length;
                  }
                })
                .with("ArrowDown", () => {
                  e.preventDefault();

                  if (selectedSignal.value == suggestions.value.length - 1) {
                    selectedSignal.value = -1;
                  } else {
                    selectedSignal.value = (selectedSignal.value + 1) %
                      suggestions.value.length;
                  }
                })
                .with("Escape", () => {
                  (e.target as HTMLInputElement).blur();
                })
                .otherwise(() => {});
            }}
          />
          <div class="flex items-center justify-center p-[2px]">
            <button
              type="submit"
              class="rounded-full h-full px-2 md:px-5 bg-brand opacity-75 text-sm text-white hover:opacity-100 transition-colors duration-50"
              style="border: none"
              title="Search"
            >
              search
            </button>
          </div>
          {suggestions.value.length > 0 &&
            (
              <div class="relative w-full col-span-full hidden group-focus-within:block">
                <div class="inset-x-4 bg-gray-200 h-px absolute -top-px" />
              </div>
            )}
          {suggestions.value.map((sug, idx) => (
            <button
              class={tx(
                "col-span-full py-1.5 pl-5 hidden group-focus-within:flex cursor-pointer hover:bg-gray-50",
                selectedSignal.value == idx ? "bg-gray-50" : "bg-white",
              )}
              onClick={(e) => {
                selectedSignal.value = idx;
                (e.target as HTMLButtonElement).form!.submit();
              }}
            >
              <div class="flex w-4 mr-3 items-center">
                <img class="h-5" loading="lazy" src="/images/search.svg" />
              </div>
              <div
                class="font-light"
                dangerouslySetInnerHTML={{ __html: sug.highlighted }}
              />
            </button>
          ))}
        </div>
      </div>

      <input
        type="hidden"
        value={safeSearchSignal.value.data ? "true" : "false"}
        name="ss"
      />
      <input type="hidden" value={compressed} name="sr" id="siteRankingsUuid" />
    </form>
  );
};
