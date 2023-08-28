import { SiteWithDelete } from "../components/Site.tsx";
import {
  generateCombinedRankingsBase64,
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
          href={site}
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

export const ClearAndExportSiteRankingButton = () => {
  const rankings = useAllRankings();

  const compressed = generateCombinedRankingsBase64(
    rankings.map(([section, { signal }]) => [section, signal.value.data]),
  );
  const url = `${window.location}/export?data=${compressed}`;

  return (
    <a
      id="export-optic"
      href={url}
      download="exported.optic"
      class="w-fit bg-brand text-white opacity-75 hover:opacity-100 transition-colors duration-50 rounded-full py-2 px-5"
      onClick={() => {
        for (const [, { signal }] of rankings) {
          updateStorageSignal(signal, () => []);
        }

        // TODO: potentially use file-saver instead
        // const fileSaver = await import("https://esm.sh/file-saver@2.0.5");
        // fileSaver.saveAs()
      }}
    >
      Clear all and export as optic
    </a>
  );
};
