import { useId } from "preact/hooks";
import { allowStatsSignal } from "../search/preferences.ts";
import {
  updateStorageSignal,
  useSyncSignalWithLocalStorage,
} from "../search/utils.ts";

export const PrivacySelector = () => {
  const id = useId();

  useSyncSignalWithLocalStorage(allowStatsSignal);

  return (
    <div class="flex space-x-1 pl-1 text-sm">
      <input
        type="checkbox"
        id={id}
        checked={allowStatsSignal.value.data}
        onChange={(e) => {
          updateStorageSignal(
            allowStatsSignal,
            () => (e.target as HTMLInputElement).checked,
          );
        }}
      />
      <label for={id}>Allow usage statistics</label>
    </div>
  );
};
