import { ComponentChildren } from "preact";
import { SiteWithDelete } from "../components/Site.tsx";
import * as search from "../search/index.ts";
import {
  generateCombinedRankings,
  SiteRankingSection,
  useAllRankings,
  useRanking,
  useSaveRanking,
} from "../search/ranking.ts";
import { updateStorageSignal } from "../search/utils.ts";
import { Button } from "../components/Button.tsx";

export const SiteRanking = ({ section }: { section: SiteRankingSection }) => {
  const ranking = useRanking(section);

  useSaveRanking(ranking);

  return (
    <div class="flex flex-wrap gap-x-5 gap-y-3" id="sites-list">
      {ranking.signal.value.data.map((site) => (
        <SiteWithDelete
          href={`https://${site}`}
          onDelete={() => {
            updateStorageSignal(
              ranking.signal,
              (sites) => sites.filter((s) => s != site),
            );
          }}
        >
          {site}
        </SiteWithDelete>
      ))}
    </div>
  );
};

export const ClearAndExportSiteRankingButton = (
  { clear = false, export: exportOptic = false, children }: {
    clear?: boolean;
    export?: boolean;
    children: ComponentChildren;
  },
) => {
  const rankings = useAllRankings();

  const combined = generateCombinedRankings(
    rankings.map(([section, { signal }]) => [section, signal.value.data]),
  );

  return (
    <Button
      onClick={async () => {
        if (exportOptic) {
          const { data } = search.api.sitesExportOptic({
            siteRankings: combined,
          });
          const { default: fileSaver } = await import("file-saver");
          fileSaver.saveAs(new Blob([await data]), "exported.optic");
        }

        if (clear) {
          for (const [, { signal }] of rankings) {
            updateStorageSignal(signal, () => []);
          }
        }
      }}
    >
      {children}
    </Button>
  );
};
