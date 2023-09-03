import { SafeSearchSettings } from "../../islands/SafeSearchSettings.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default function Settings() {
  return (
    <div class="[&_a]:underline [&_a]:font-medium">
      <div class="space-y-3">
        <h1 class="text-2xl font-medium">Preferences</h1>
        <div>
          <SafeSearchSettings />
        </div>
      </div>
    </div>
  );
}
