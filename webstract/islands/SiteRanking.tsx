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
    <button
      class="w-fit bg-brand text-white opacity-75 hover:opacity-100 transition-colors duration-50 rounded-full py-2 px-5"
      onClick={async () => {
        if (exportOptic) {
          const { data } = search.api.sitesExportOptic({
            siteRankings: combined,
          });
          const { default: fileSaver } = await import(
            "https://esm.sh/file-saver@2.0.5"
          );
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
    </button>
  );
};
