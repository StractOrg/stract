import { safeSearchSignal } from "../search/preferences.ts";
import {
  updateStorageSignal,
  useSyncSignalWithLocalStorage,
} from "../search/utils.ts";

export const SafeSearchSettings = () => {
  useSyncSignalWithLocalStorage(safeSearchSignal);

  return (
    <div>
      <h2 class="text-lg">Safe Search</h2>
      <div class="flex flex-col sm:flex-row sm:justify-between pr-5 space-y-1 sm:space-y-0">
        <p>Remove explicit content from search results</p>
        <div class="flex space-x-4">
          {([["on", true], ["off", false]] as const).map(([name, state]) => (
            <label
              class="cursor-pointer flex flex-col items-center"
              for={`safe-search-${state}`}
            >
              <input
                type="radio"
                name="safe-search"
                id={`safe-search-${state}`}
                value={name}
                checked={safeSearchSignal.value.data == state}
                onChange={(e) =>
                  updateStorageSignal(safeSearchSignal, () =>
                    (e.target as HTMLInputElement).checked == state)}
              />
              <span class="capitalize">{name}</span>
            </label>
          ))}
        </div>
      </div>

      <p class="pt-5 text-neutral-300">More to come soon</p>
    </div>
  );
};
