import {
  ClearAndExportSiteRankingButton,
  SiteRanking,
} from "../../islands/SiteRanking.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default function Sites() {
  return (
    <div class="flex flex-col w-full h-full space-y-10">
      <div class="space-y-2">
        <h1 class="text-2xl font-medium">Liked Sites</h1>
        <div class="text-sm">
          Sites that are similar to these sites receive a boost during search.
          Their results are more likely to appear in your search results.
        </div>
        <div>
          <SiteRanking section="liked" />
        </div>
      </div>

      <div class="space-y-2">
        <h1 class="text-2xl font-medium">Disliked Sites</h1>
        <div class="text-sm">
          Sites that are similar to these sites gets de-prioritized during
          search. Their results are less likely to appear in your search
          results.
        </div>
        <div>
          <SiteRanking section="disliked" />
        </div>
      </div>

      <div class="space-y-2">
        <h1 class="text-2xl font-medium">Blocked Sites</h1>
        <div class="text-sm">
          These are the sites you have blocked. They won't appear in any of your
          searches.
        </div>
        <div>
          <SiteRanking section="blocked" />
        </div>
      </div>

      <div class="flex justify-center">
        <ClearAndExportSiteRankingButton />
      </div>
    </div>
  );
}
