<script lang="ts">
  import { api } from '$lib/api';
  import Button from '$lib/components/Button.svelte';
  import Site from '$lib/components/Site.svelte';
  import { getButtonTailwindStyle} from '$lib/themes';
  import { rankingsToRanked, type RankedSites, Ranking } from '$lib/rankings';
  import { hostRankingsStore } from '$lib/stores';
  import { flip } from 'svelte/animate';
  import { derived } from 'svelte/store';
  import init, {Optic} from 'client-wasm';
  import { onMount } from 'svelte';

  onMount(async () => {
    // Initialize the wasm module
    await init();
  })

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

  // Called when the user selects an optic file for import
  const importOpticFile = (e: Event) => {
    // Get an array of the uploaded files
    let files: File[] = [...(<HTMLInputElement>e.target)?.files ?? new FileList]

    // Iterate through all files, attempt to get the contents & parse the optic
    files.forEach((file) => {
      if (file) {
        const reader = new FileReader();
        reader.readAsText(file,'UTF-8');

        reader.onload = readerEvent => {
          const content = readerEvent.target?.result ?? "";
          try {
            const extractedRankings: RankedSites = JSON.parse(Optic.parsePreferenceOptic(content as string));
            // Iterate through all sites in each Ranking and pass them to rankSite
            for (const [_, ranking] of Object.entries(Ranking)) {
              const sites = extractedRankings[ranking]
              sites.forEach((site) => rankSite(site, ranking))
            }
          } catch {
            console.error(`Failed to import optic from "${file.name}", please check the formatting.`)
          }
        }
      }
    })
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
    <input type="file" accept=".optic" id="optic-import" multiple on:change={importOpticFile} hidden/>
    <label
      for="optic-import"
      class={getButtonTailwindStyle(false, false, 'primary', '')}
    >
      Import from optic
    </label>
  </div>
</div>
