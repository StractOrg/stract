import { useSignal } from "@preact/signals";
import { DEFAULT_OPTICS, OpticOption, opticsSignal } from "../search/optics.ts";
import { IS_BROWSER } from "$fresh/runtime.ts";
import {
  updateStorageSignal,
  useSyncSignalWithLocalStorage,
} from "../search/utils.ts";
import { tx } from "https://esm.sh/@twind/core@1.1.3";

export const ManageOptics = () => {
  const newOptic = useSignal<OpticOption>({
    name: "",
    url: "",
    description: "",
  });

  useSyncSignalWithLocalStorage(opticsSignal);

  const inputClass = tx`border-none rounded`;

  return (
    <div>
      {IS_BROWSER &&
        (
          <>
            <div class="flex w-full justify-between px-5 h-10">
              <input
                class={inputClass}
                id="name"
                type="text"
                placeholder="Name"
                value={newOptic.value.name}
                onInput={(e) =>
                  newOptic.value.name = (e.target as HTMLInputElement).value}
              />
              <input
                class={inputClass}
                id="url"
                type="text"
                placeholder="Url"
                value={newOptic.value.url}
                onInput={(e) =>
                  newOptic.value.url = (e.target as HTMLInputElement).value}
              />
              <button
                class="w-20 rounded-full border-0 bg-brand text-sm text-white"
                id="add-btn"
                onClick={() => {
                  updateStorageSignal(
                    opticsSignal,
                    (optics) => [...optics, newOptic.value],
                  );
                  newOptic.value = {
                    name: "",
                    url: "",
                    description: "",
                  };
                }}
              >
                Add
              </button>
            </div>
            <div class="flex w-full px-5 h-10 mt-2">
              <input
                class={inputClass}
                id="description"
                type="text"
                placeholder="Description"
                value={newOptic.value.description}
                onInput={(e) =>
                  newOptic.value.description =
                    (e.target as HTMLInputElement).value}
              />
            </div>
          </>
        )}
      <div class="mt-5">
        <div
          class="grid grid-cols-[auto_1fr_1fr_1fr] w-full gap-5"
          id="optics-list"
        >
          <span />
          <div class="flex-1 font-medium">Name</div>
          <div class="flex-1 font-medium">Description</div>
          <div class="flex-1 font-medium">Link</div>
          {[
            ...opticsSignal.value.data.map((optic) => ({
              optic,
              img: "/images/delete.svg",
            })),
            ...DEFAULT_OPTICS.map((optic) => ({
              optic,
              img: "/images/disabled-delete.svg",
            })),
          ].map(({ optic, img }) => (
            <>
              <div
                class="w-5"
                onClick={() =>
                  updateStorageSignal(
                    opticsSignal,
                    (optics) => optics.filter((o) => o != optic),
                  )}
              >
                <img src={img} class="h-5 w-5" />
              </div>
              <div class="text-sm">{optic.name}</div>
              <div class="text-sm">
                {optic.description}
              </div>
              <div class="text-sm">
                <a href={optic.url}>Source</a>
              </div>
            </>
          ))}
        </div>
      </div>
    </div>
  );
};
