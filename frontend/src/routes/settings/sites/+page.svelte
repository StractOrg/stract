<script lang="ts">
  import { api } from '$lib/api';
  import Button from '$lib/components/Button.svelte';
  import Site from '$lib/components/Site.svelte';
  import { rankingsToRanked, importedOpticToRankings, Ranking } from '$lib/rankings';
  import { hostRankingsStore } from '$lib/stores';
  import { flip } from 'svelte/animate';
  import { derived } from 'svelte/store';

  const sections = [
    {
      title: 'Liked Sites',
      description:
        'Sites that are similar to these sites receive a boost during search. Their results are more likely to appear in your search results.',
      section: Ranking.LIKED,
    },
    {
      title: 'Disliked Sites',
      description:
        'Sites that are similar to these sites gets de-prioritized during search. Their results are less likely to appear in your search results.',
      section: Ranking.DISLIKED,
    },

    {
      title: 'Blocked Sites',
      description:
        "These are the sites you have blocked. They won't appear in any of your searches.",
      section: Ranking.BLOCKED,
    },
  ] as const;

  const buttons = [
    { text: 'Export as optic', clear: false },
    { text: 'Clear and export as optic', clear: true },
  ] as const;

  const ranked = derived(hostRankingsStore, ($rankings) => rankingsToRanked($rankings));

  // Allow the user to import a .optic file
  const importOpticFile = () => {
    // Select the "optic-import" input field in the DOM
    const input = document.getElementById("optic-import")

    if (input){
      input.onchange = e => { 
        // Get an array of the uploaded files
        let files: File[] = [...(<HTMLInputElement>e.target)?.files ?? new FileList]

        // Iterate through all files, attempt to get the contents & parse the optic
        files.forEach((file) => {
          if (file) {
            const reader = new FileReader();
            reader.readAsText(file,'UTF-8');

            reader.onload = readerEvent => {
              const content = readerEvent.target?.result ?? "";
              const extractedRankings = importedOpticToRankings(content as string);
              // Iterate through SiteRecords and pass each site/rank to rankSite
              for (const site in extractedRankings) {
                const rank = extractedRankings[site]
                if (rank) rankSite(site, rank)
              }
            }
          }
        })
      }
      // Activate the input (bring up file prompt)
      input.click()
    }
  }

  const rankSite = (site: string, ranking: Ranking) => {
    hostRankingsStore?.update(($rankings) => ({
      ...$rankings,
      [site]: $rankings[site] == ranking ? void 0 : ranking,
    }));
  };

  const unrankSite = (site: string) => () => {
    hostRankingsStore.update(($rankings) => ({ ...$rankings, [site]: void 0 }));
  };

  const clearAndExport =
    ({ clear }: { clear: boolean }) =>
    async () => {
      const { data } = api.hostsExport({
        hostRankings: $ranked,
      });
      const { default: fileSaver } = await import('file-saver');
      fileSaver.saveAs(new Blob([await data]), 'exported.optic');

      if (clear) hostRankingsStore.set({});
    };
</script>

<div class="flex flex-col space-y-10">
  {#each sections as { title, description, section }}
    <div class="space-y-2">
      <h1 class="text-2xl font-medium">{title}</h1>
      <div class="text-sm">{description}</div>
      <div class="flex flex-wrap gap-x-5 gap-y-3" id="sites-list">
        {#each $ranked[section] as site (site)}
          <div animate:flip={{ duration: 150 }}>
            <Site href="http://{site}" on:delete={unrankSite(site)}>
              {site}
            </Site>
          </div>
        {/each}
      </div>
    </div>
  {/each}

  <div class="flex justify-center space-x-4">
    {#each buttons as { text, clear }}
      <Button on:click={clearAndExport({ clear })}>{text}</Button>
    {/each}
    <input type="file" accept=".optic" id="optic-import" multiple hidden/>
    <Button on:click={importOpticFile}>Import from optic</Button>
  </div>
</div>
