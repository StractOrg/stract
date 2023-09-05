import { useSignal } from "@preact/signals";
import { DEFAULT_OPTICS, OpticOption, opticsSignal } from "../search/optics.ts";
import { IS_BROWSER } from "$fresh/runtime.ts";
import {
  updateStorageSignal,
  useSyncSignalWithLocalStorage,
} from "../search/utils.ts";
import { HiMinusCircleOutline } from "../icons/HiMinusCircleOutline.tsx";
import { Button } from "../components/Button.tsx";

export const ManageOptics = () => {
  const newOptic = useSignal<OpticOption>({
    name: "",
    url: "",
    description: "",
  });

  useSyncSignalWithLocalStorage(opticsSignal);

  return (
    <div>
      {IS_BROWSER &&
        (
          <form
            class="grid w-full px-8 grid-cols-[1fr_1fr_5rem] gap-x-5 gap-y-2 [&>input]:border-none [&>input]:rounded [&>input]:bg-transparent"
            onSubmit={(e) => {
              e.preventDefault();
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
            <input
              type="text"
              required
              placeholder="Name"
              value={newOptic.value.name}
              onInput={(e) =>
                newOptic.value.name = (e.target as HTMLInputElement).value}
            />
            <input
              type="text"
              required
              placeholder="Url"
              value={newOptic.value.url}
              onInput={(e) =>
                newOptic.value.url = (e.target as HTMLInputElement).value}
            />
            <Button title="Remove optic">
              Add
            </Button>
            <input
              class="col-span-2"
              type="text"
              required
              placeholder="Description"
              value={newOptic.value.description}
              onInput={(e) =>
                newOptic.value.description =
                  (e.target as HTMLInputElement).value}
            />
          </form>
        )}
      <div class="mt-5">
        <div
          class="grid grid-cols-[auto_1fr_2fr_auto] w-full gap-5"
          id="optics-list"
        >
          <span />
          <div class="flex-1 font-medium">Name</div>
          <div class="flex-1 font-medium">Description</div>
          <div class="flex-1 font-medium">Link</div>
          {[
            ...opticsSignal.value.data.map((optic) => ({
              optic,
              removable: true,
            })),
            ...DEFAULT_OPTICS.map((optic) => ({
              optic,
              removable: false,
            })),
          ].map(({ optic, removable }) => (
            <>
              <button
                class="w-6 !bg-transparent flex items-start group"
                onClick={() =>
                  updateStorageSignal(
                    opticsSignal,
                    (optics) => optics.filter((o) => o != optic),
                  )}
                disabled={!removable}
              >
                <HiMinusCircleOutline class="group-enabled:text-red-500 group-enabled:group-hover:text-red-400 transition" />
              </button>
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
