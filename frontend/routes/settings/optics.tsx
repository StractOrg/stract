import { ManageOptics } from "../../islands/ManageOptics.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default function Optics() {
  return (
    <div class="[&_a]:underline [&_a]:font-medium">
      <div class="space-y-3">
        <h1 class="text-2xl font-medium">Manage Optics</h1>
        <div class="text-sm">
          Optics lets you control almost everything about which search results
          that gets returned to you. You can discard results from specific
          sites, boost results from other sites and much more.
        </div>
        <div class="text-sm">
          See our{" "}
          <a href="https://github.com/StractOrg/sample-optics/blob/main/quickstart.optic">
            quickstart
          </a>{" "}
          for how to get started. Once you have developed your optic, you can
          add it here to be used during your search.
        </div>
        <div class="text-sm">
          Simply host the optic on a url that returns a plain-text HTTP response
          with the optic. We use raw.githubusercontent.com, but you are free to
          host them elsewhere.
        </div>
      </div>
      <div class="mt-16">
        <ManageOptics />
      </div>
    </div>
  );
}
