<script lang="ts">
  import { api } from '$lib/api';
  import Button from '$lib/components/Button.svelte';
  import Site from '$lib/components/Site.svelte';
  import { rankingsToRanked } from '$lib/rankings';
  import { siteRankingsStore } from '$lib/stores';
  import { flip } from 'svelte/animate';
  import { derived } from 'svelte/store';

  const sections = [
    {
      title: 'Liked Sites',
      description:
        'Sites that are similar to these sites receive a boost during search. Their results are more likely to appear in your search results.',
      section: 'liked',
    },
    {
      title: 'Disliked Sites',
      description:
        'Sites that are similar to these sites gets de-prioritized during search. Their results are less likely to appear in your search results.',
      section: 'disliked',
    },

    {
      title: 'Blocked Sites',
      description:
        "These are the sites you have blocked. They won't appear in any of your searches.",
      section: 'blocked',
    },
  ] as const;

  const buttons = [
    { text: 'Export as optic', clear: false },
    { text: 'Clear and export as optic', clear: true },
  ] as const;

  const ranked = derived(siteRankingsStore, ($rankings) => rankingsToRanked($rankings));

  const unrankSite = (site: string) => () => {
    siteRankingsStore.update(($rankings) => ({ ...$rankings, [site]: void 0 }));
  };
  const clearAndExport =
    ({ clear }: { clear: boolean }) =>
    async () => {
      const { data } = api.sitesExportOptic({
        siteRankings: $ranked,
      });
      const { default: fileSaver } = await import('file-saver');
      fileSaver.saveAs(new Blob([await data]), 'exported.optic');

      if (clear) siteRankingsStore.set({});
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
            <Site href="https://{site}" on:delete={unrankSite(site)}>
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
  </div>
</div>
