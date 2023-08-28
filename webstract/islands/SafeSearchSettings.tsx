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
          <div>
            <input
              type="radio"
              name="safe-search"
              id="safe-search-on"
              value="on"
              checked={safeSearchSignal.value.data}
              onChange={(e) =>
                updateStorageSignal(safeSearchSignal, () =>
                  (e.target as HTMLInputElement).checked)}
            />
            <label for="safe-search-on">On</label>
          </div>
          <div>
            <input
              type="radio"
              name="safe-search"
              id="safe-search-off"
              value="off"
              checked={!safeSearchSignal.value.data}
              onChange={(e) =>
                updateStorageSignal(safeSearchSignal, () =>
                  !(e.target as HTMLInputElement).checked)}
            />
            <label for="safe-search-off">Off</label>
          </div>
        </div>
      </div>

      <p class="pt-5 text-neutral-300">More to come soon</p>
    </div>
  );
};
