<script lang="ts">
  import type { Ranking } from '$lib/rankings';
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
  let top = modal.top + 32;

  $: {
    if (innerWidth < mediumWidthCutoff) {
      left = modal.left - (widthPixels + 16);
    } else {
      left = modal.left + 16;
    }
  }

  const rankingChoices = [
    {
      ranking: 'liked',
      kind: 'success',
      Icon: HandThumbUp,
    },
    {
      ranking: 'disliked',
      kind: 'warning',
      Icon: HandThumbDown,
    },
    {
      ranking: 'blocked',
      kind: 'error',
      Icon: NoSymbol,
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
    <h2 class="w-fit text-center">
    Do you like results from {modal.site.site}?
    </h2>
    <div class="flex justify-center space-x-1.5 pt-2">
    {#each rankingChoices as { ranking, kind, Icon }}
        <Button
        {kind}
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