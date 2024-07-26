<script context="module" lang="ts">
  export type SignalLabel =
    | 'combinedText'
    | 'title'
    | 'body'
    | 'keywords'
    | 'backlink_text'
    | 'url'
    | 'site'
    | 'domain'
    | 'hostCentrality'
    | 'pageCentrality'
    | 'isHomepage'
    | 'fetchTime'
    | 'updateTimestamp'
    | 'trackerScore'
    | 'region'
    | 'queryCentrality'
    | 'inboundSimilarity'
    | 'urlSymbols'
    | 'linkDensity';
</script>

<script lang="ts">
  import type { Ranking } from '$lib/rankings';
  import { hostRankingsStore } from '$lib/stores';
  import type { DisplayedWebpage, SignalEnumDiscriminants, SignalScore } from '$lib/api';
  import { twJoin } from 'tailwind-merge';
  import Button from '$lib/components/Button.svelte';
  import HandThumbDown from '~icons/heroicons/hand-thumb-down-20-solid';
  import HandThumbUp from '~icons/heroicons/hand-thumb-up-20-solid';
  import NoSymbol from '~icons/heroicons/no-symbol-20-solid';
  import { scale } from 'svelte/transition';
  import { match } from 'ts-pattern';
  import SignalInfo from './SignalInfo.svelte';

  export let modal: { top: number; left: number; site: DisplayedWebpage };

  const mediumWidthCutoff = 768;

  const hasSignals = Object.keys(modal.site.rankingSignals ?? {}).length;
  const widthPixels = hasSignals > 0 ? 300 : 208;

  let innerWidth = 0;

  let left = modal.left;

  const signalLabel = (signal: SignalEnumDiscriminants): SignalLabel | undefined => {
    return match(signal)
      .with('bm25_title', () => 'title' as const)
      .with('bm25_title_bigrams', () => 'title' as const)
      .with('bm25_title_trigrams', () => 'title' as const)
      .with('bm25_clean_body', () => 'body' as const)
      .with('bm25_clean_body_bigrams', () => 'body' as const)
      .with('bm25_clean_body_trigrams', () => 'body' as const)
      .with('bm25_stemmed_title', () => 'title' as const)
      .with('bm25_stemmed_clean_body', () => 'body' as const)
      .with('bm25_all_body', () => 'body' as const)
      .with('bm25_keywords', () => 'keywords' as const)
      .with('bm25_backlink_text', () => 'backlink_text' as const)
      .with('idf_sum_url', () => 'url' as const)
      .with('idf_sum_site', () => 'site' as const)
      .with('idf_sum_domain', () => 'domain' as const)
      .with('idf_sum_site_no_tokenizer', () => 'site' as const)
      .with('idf_sum_domain_no_tokenizer', () => 'domain' as const)
      .with('idf_sum_domain_name_no_tokenizer', () => 'domain' as const)
      .with('idf_sum_domain_if_homepage', () => 'domain' as const)
      .with('idf_sum_domain_name_if_homepage_no_tokenizer', () => 'domain' as const)
      .with('idf_sum_domain_if_homepage_no_tokenizer', () => 'domain' as const)
      .with('idf_sum_title_if_homepage', () => 'title' as const)
      .with('cross_encoder_snippet', () => 'body' as const)
      .with('cross_encoder_title', () => 'title' as const)
      .with('host_centrality', () => 'hostCentrality' as const)
      .with('host_centrality_rank', () => 'hostCentrality' as const)
      .with('page_centrality', () => 'pageCentrality' as const)
      .with('page_centrality_rank', () => 'pageCentrality' as const)
      .with('is_homepage', () => 'isHomepage' as const)
      .with('fetch_time_ms', () => 'fetchTime' as const)
      .with('update_timestamp', () => 'updateTimestamp' as const)
      .with('tracker_score', () => 'trackerScore' as const)
      .with('region', () => 'region' as const)
      .with('query_centrality', () => 'queryCentrality' as const)
      .with('inbound_similarity', () => 'inboundSimilarity' as const)
      .with('lambda_mart', () => undefined)
      .with('url_digits', () => 'urlSymbols' as const)
      .with('url_slashes', () => 'urlSymbols' as const)
      .with('link_density', () => 'linkDensity' as const)
      .with('title_embedding_similarity', () => 'title' as const)
      .with('keyword_embedding_similarity', () => 'keywords' as const)
      .with('bm25_f', () => 'combinedText' as const)
      .exhaustive();
  };

  type SignalGroup = {
    label: SignalLabel;
    signals: [SignalEnumDiscriminants, SignalScore][];
  };

  const score = (group: SignalGroup): number => {
    return group.signals.reduce((acc, [_, score]) => {
      return acc + score.coefficient * score.value;
    }, 0);
  };

  $: signalGroups = Object.entries(modal.site.rankingSignals ?? {}).reduce(
    (acc, [untypedSignal, untypedScore]) => {
      const signal = untypedSignal as SignalEnumDiscriminants;
      const score = untypedScore as SignalScore;

      const label = signalLabel(signal);
      if (label) {
        const group = acc.find((group) => group.label === label);
        if (group) {
          group.signals.push([signal, score]);
        } else {
          acc.push({ label, signals: [[signal, score]] });
        }
      }
      return acc;
    },
    [] as SignalGroup[],
  );

  $: {
    signalGroups.sort((a, b) => score(b) - score(a));

    for (const group of signalGroups) {
      group.signals.sort((a, b) => b[1].coefficient * b[1].value - a[1].coefficient * a[1].value);
    }
  }

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
      ranking: 'liked',
      kind: 'success',
      Icon: HandThumbUp,
      title: 'Like Site',
      aria_label: 'I want more results like this site.',
    },
    {
      ranking: 'disliked',
      kind: 'warning',
      Icon: HandThumbDown,
      title: 'Dislike Site',
      aria_label: 'I want fewer results like this site.',
    },
    {
      ranking: 'blocked',
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
    <p class="w-full break-all text-center">
      {modal.site.site}?
    </p>
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
    {#if signalGroups.length > 0}
      <h3 class="mt-4 font-medium">Ranking Explanation</h3>
      <div class="flex flex-col gap-y-1">
        {#each signalGroups.slice(0, 3) as { label, signals }}
          <SignalInfo {label} {signals} />
        {/each}
      </div>
    {/if}
  </div>
</div>
