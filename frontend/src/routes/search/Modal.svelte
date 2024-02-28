<script lang="ts">
  import { Ranking } from '$lib/rankings';
  import { hostRankingsStore, summarize } from '$lib/stores';
  import type { DisplayedWebpage } from '$lib/api';
  import { twJoin } from 'tailwind-merge';
  import Button from '$lib/components/Button.svelte';
  import HandThumbDown from '~icons/heroicons/hand-thumb-down-20-solid';
  import HandThumbUp from '~icons/heroicons/hand-thumb-up-20-solid';
  import NoSymbol from '~icons/heroicons/no-symbol-20-solid';
  import { scale } from 'svelte/transition';

  export let query: string;
  export let modal: { top: number; left: number; site: DisplayedWebpage };

  const mediumWidthCutoff = 768;

  const widthPixels = 208;

  let innerWidth = 0;

  let left = modal.left;
  $: top = modal.top + 32;

  $: {
    if (innerWidth < mediumWidthCutoff) {
      left = modal.left - (widthPixels + 16);
    } else {
      left = modal.left + 16;
    }
  }

  const rankingChoices = [
    {
      ranking: Ranking.LIKED,
      kind: 'success',
      Icon: HandThumbUp,
      title: 'Like Site',
      aria_label: 'I want more results like this site.',
    },
    {
      ranking: Ranking.DISLIKED,
      kind: 'warning',
      Icon: HandThumbDown,
      title: 'Dislike Site',
      aria_label: 'I want fewer results like this site.',
    },
    {
      ranking: Ranking.BLOCKED,
      kind: 'error',
      Icon: NoSymbol,
      title: 'Block Site',
      aria_label: 'I never want to see results from this site.',
    },
  ] as const;

  const rankSite = (site: DisplayedWebpage, ranking: Ranking) => () => {
    hostRankingsStore?.update(($rankings) => ({
      ...$rankings,
      [site.site]: $rankings[site.site] == ranking ? void 0 : ranking,
    }));
  };

  const summarizeSite = (site: DisplayedWebpage) => () => summarize(query, site);
</script>

<svelte:window bind:innerWidth />

<!-- svelte-ignore a11y-click-events-have-key-events -->
<!-- svelte-ignore a11y-no-static-element-interactions -->
<div
  class={twJoin(
    'absolute -translate-y-1/2 transition-all',
    'h-fit flex-col items-center overflow-hidden rounded-lg border bg-base-100 px-2 py-5 text-sm drop-shadow-md',
  )}
  style="top: {top}px; left: calc({left}px); width: {widthPixels}px;"
  transition:scale={{ duration: 150 }}
  on:click|stopPropagation={() => {}}
>
  <div>
    <h2 class="w-full text-center">Do you like results from</h2>
    <p class="text-center">{modal.site.site}?</p>
    <div class="flex justify-center space-x-1.5 pt-2">
      {#each rankingChoices as { ranking, kind, Icon, title, aria_label }}
        <Button
          {kind}
          {title}
          {aria_label}
          pale={$hostRankingsStore[modal.site.site] != ranking}
          padding={false}
          form="searchbar-form"
          on:click={rankSite(modal.site, ranking)}
        >
          <Icon class="w-4" />
        </Button>
      {/each}
    </div>
    <div class="mt-4 flex justify-center">
      <Button pale on:click={summarizeSite(modal.site)}>Summarize Result</Button>
    </div>
  </div>
</div>
