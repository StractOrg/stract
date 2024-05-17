<script lang="ts">
  import ChevronDown from '~icons/heroicons/chevron-down';
  import { match } from 'ts-pattern';
  import type { SignalLabel } from './Modal.svelte';
  import type { SignalEnumDiscriminants, SignalScore } from '$lib/api';

  export let label: SignalLabel;
  export let signals: [SignalEnumDiscriminants, SignalScore][];

  type SignalInfo = {
    title: string;
    description: string;
  };
  const signalInfo = (label: SignalLabel): SignalInfo => {
    return match(label)
      .with('title', () => ({ title: 'Title', description: 'The title of the webpage.' }))
      .with('body', () => ({ title: 'Body', description: 'The body of the webpage.' }))
      .with('keywords', () => ({
        title: 'Keywords',
        description: 'Keywords extracted from the webpage.',
      }))
      .with('backlink_text', () => ({
        title: 'Backlink Text',
        description: 'Text of the backlink to the webpage.',
      }))
      .with('url', () => ({ title: 'URL', description: 'The URL of the webpage.' }))
      .with('site', () => ({ title: 'Site', description: 'The site of the webpage.' }))
      .with('domain', () => ({ title: 'Domain', description: 'The domain of the webpage.' }))
      .with('hostCentrality', () => ({
        title: 'Host Centrality',
        description: 'The centrality of the host.',
      }))
      .with('pageCentrality', () => ({
        title: 'Page Centrality',
        description: 'The centrality of the page.',
      }))
      .with('isHomepage', () => ({
        title: 'Is Homepage',
        description: 'Whether the webpage is the homepage of the site.',
      }))
      .with('fetchTime', () => ({
        title: 'Fetch Time',
        description: 'The time it took to fetch the webpage.',
      }))
      .with('updateTimestamp', () => ({
        title: 'Update Timestamp',
        description: 'The time the webpage was last updated.',
      }))
      .with('trackerScore', () => ({
        title: 'Tracker Score',
        description: 'Score to determine if a page has few trackers.',
      }))
      .with('region', () => ({ title: 'Region', description: 'The region of the webpage.' }))
      .with('queryCentrality', () => ({
        title: '',
        description: '',
      }))
      .with('inboundSimilarity', () => ({
        title: '',
        description: '',
      }))
      .with('urlSymbols', () => ({ title: '', description: '' }))
      .with('linkDensity', () => ({
        title: 'Link Density',
        description: 'The density of links in the webpage.',
      }))
      .exhaustive();
  };
</script>

<details class="group">
  <summary class="flex cursor-pointer list-none space-x-1">
    <span>
      <ChevronDown class="w-3 text-sm transition group-open:rotate-180" />
    </span>
    <div>
      {signalInfo(label).title}
    </div>
  </summary>
  <p class="text-sm">{signalInfo(label).description}</p>
  <div class="mb-2 mt-1">
    <div class="flex flex-wrap gap-x-1 gap-y-1">
      {#each signals.slice(0, 3) as [signal, score]}
        <span
          class="rounded-full bg-base-200 px-2 text-center text-xs text-neutral-focus"
          title="score: {(score.value * score.coefficient).toFixed(2)}"
        >
          {signal.replace(/_/g, '-')}
        </span>
      {/each}
    </div>
  </div>
</details>
