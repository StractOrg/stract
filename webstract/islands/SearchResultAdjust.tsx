import { Signal } from "@preact/signals";
import * as search from "../search/index.ts";
import { useEffect } from "preact/hooks";
import { IS_BROWSER } from "$fresh/runtime.ts";
import { ComponentChildren } from "preact";
import {
  RankingSignal,
  useRanking,
  useSaveRanking,
} from "../search/ranking.ts";
import { match } from "ts-pattern";
import { summarySignals } from "../search/summary.ts";
import { updateStorageSignal } from "../search/utils.ts";
import { HiAdjustmentsVerticalOutline } from "../icons/HiAdjustmentsVerticalOutline.tsx";
import { HiHandThumbUpOutline } from "../icons/HiHandThumbUpOutline.tsx";
import { HiHandThumbDownOutline } from "../icons/HiHandThumbDownOutline.tsx";
import { HiNoSymbol } from "../icons/HiNoSymbol.tsx";
import { tx } from "https://esm.sh/@twind/core@1.1.3";

export type SelectedAdjust =
  | { button: HTMLElement; item: search.Webpage }
  | null;

export default function SearchResultAdjust(
  { item, selected }: {
    item: search.Webpage;
    selected: Signal<SelectedAdjust>;
  },
) {
  if (!IS_BROWSER) return null;

  return (
    <div
      class="adjust-button hidden min-w-fit items-center justify-center md:flex w-5 text-gray-500/25 hover:cursor-pointer hover:text-gray-500 bg-transparent"
      data-site={item.site}
      data-url={item.url}
      onClick={(e) => {
        e.stopPropagation();
        selected.value = { button: e.currentTarget as HTMLElement, item };
      }}
    >
      <HiAdjustmentsVerticalOutline class="w-6" />
    </div>
  );
}

export const SearchResultAdjustModal = (
  { query, selected }: { query: string; selected: Signal<SelectedAdjust> },
) => {
  useEffect(() => {
    const listener = () => {
      selected.value = null;
    };
    document.addEventListener("click", listener);
    return () => {
      document.removeEventListener("click", listener);
    };
  }, [selected]);

  const rect = selected.value
    ? selected.value.button.getBoundingClientRect()
    : void 0;
  const rankingModalHeight = 0;

  if (!IS_BROWSER) return null;

  const liked = useRanking("liked");
  const disliked = useRanking("disliked");
  const blocked = useRanking("blocked");

  useSaveRanking(liked);
  useSaveRanking(disliked);
  useSaveRanking(blocked);

  return (
    <div
      class={tx(
        "absolute h-fit w-52 origin-left flex-col items-center overflow-hidden rounded-lg border bg-white py-5 px-2 text-sm drop-shadow-md -translate-y-1/2",
        rect ? "flex transition-all scale-1" : "scale-0",
      )}
      id="modal"
      style={rect && {
        left: rect.left + rect.width + 5 + "px",
        top: (rect.top + rect.bottom) / 2 + document.documentElement.scrollTop -
          rankingModalHeight / 2 + "px",
      }}
      onClick={(e) => {
        e.stopPropagation();
      }}
    >
      <div>
        <h2 class="w-fit text-center">
          Do you like results from{" "}
          {selected.value ? selected.value.item.domain : "EXAMPLE.com"}?
        </h2>
        <div class="flex space-x-1.5 pt-2 justify-center">
          <span class="text-brand">
            <AdjustButton
              ranking={liked}
              others={[disliked, blocked]}
              selected={selected}
            >
              <HiHandThumbUpOutline class="w-4" />
            </AdjustButton>
          </span>
          <span class="text-amber-400">
            <AdjustButton
              ranking={disliked}
              others={[liked, blocked]}
              selected={selected}
            >
              <HiHandThumbDownOutline class="w-4" />
            </AdjustButton>
          </span>
          <span class="text-red-500">
            <AdjustButton
              ranking={blocked}
              others={[liked, disliked]}
              selected={selected}
            >
              <HiNoSymbol class="w-4" />
            </AdjustButton>
          </span>
        </div>
      </div>
      <div class="mt-4 flex justify-center">
        <button
          className={tx(
            "rounded-full px-2 py-1 border bg-white",
            // #summarize-btn:not(:disabled)
            "hover:border-brand hover:text-brand",
            "[&.active]:border-brand [&.active]:text-brand",
          )}
          onClick={() => {
            const item = selected.value?.item;
            if (!item) return;

            search.api.summarize({ query, url: item.url }, (e) => {
              summarySignals.value = match(e)
                .with({ type: "begin" }, () => ({
                  ...summarySignals.value,
                  [item.url]: { inProgress: true, data: "" },
                }))
                .with({ type: "content" }, ({ data }) => ({
                  ...summarySignals.value,
                  [item.url]: {
                    inProgress: true,
                    data: (summarySignals.value[item.url]?.data ?? "") + data,
                  },
                }))
                .with({ type: "done" }, () => ({
                  ...summarySignals.value,
                  [item.url]: {
                    inProgress: false,
                    data: (summarySignals.value[item.url]?.data ?? ""),
                  },
                })).exhaustive();
            });
          }}
        >
          Summarize Result
        </button>
      </div>
    </div>
  );
};

function AdjustButton(
  { ranking, others, selected, children }: {
    ranking: RankingSignal;
    others: RankingSignal[];
    selected: Signal<SelectedAdjust>;
    children: ComponentChildren;
  },
) {
  const active = ranking.signal.value.data.includes(
    selected.value?.item.domain ?? " selected",
  );

  return (
    <button
      class={tx(
        "group rounded-full border bg-white px-2 py-2",
        "hover:border-current hover:text-current",
        active && "border-current text-inherit",
      )}
      onClick={() => {
        const domain = selected.value?.item.domain;
        if (!domain) return;

        if (active) {
          updateStorageSignal(ranking.signal, (sites) =>
            sites.filter((s) => s != domain));
        } else {
          updateStorageSignal(ranking.signal, (sites) => [...sites, domain]);
        }
        for (const other of others) {
          updateStorageSignal(other.signal, (sites) =>
            sites.filter((s) =>
              s != domain
            ));
        }
      }}
    >
      <span
        class={tx(
          active ? "text-inherit" : "text-[#333]",
          "group-hover:text-inherit",
        )}
      >
        {children}
      </span>
    </button>
  );
}
