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
      .with('title', () => ({
        title: 'Title',
        description: 'The query seems to match the title of the webpage',
      }))
      .with('body', () => ({
        title: 'Body',
        description: 'The query seems to match the text body of the webpage',
      }))
      .with('keywords', () => ({
        title: 'Keywords',
        description: 'The page contains keywords that also appear in the query',
      }))
      .with('backlink_text', () => ({
        title: 'Backlink Text',
        description: 'Pages linking to this page contain the query in their anchor text',
      }))
      .with('url', () => ({ title: 'URL', description: 'Words from the query appear in the URL' }))
      .with('site', () => ({
        title: 'Site',
        description: 'The query matches the site of the page',
      }))
      .with('domain', () => ({
        title: 'Domain',
        description: 'The query matches the domain of the page',
      }))
      .with('hostCentrality', () => ({
        title: 'Host Centrality',
        description: 'The host of the page has a high centrality score in the web graph',
      }))
      .with('pageCentrality', () => ({
        title: 'Page Centrality',
        description: 'The page has a high centrality score in the web graph',
      }))
      .with('isHomepage', () => ({
        title: 'Is Homepage',
        description: 'The page is the homepage of the site',
      }))
      .with('fetchTime', () => ({
        title: 'Fetch Time',
        description:
          'The page had a low fetch time, indicating you will get a good user experience on the page',
      }))
      .with('updateTimestamp', () => ({
        title: 'Update Timestamp',
        description: 'The page was recently updated',
      }))
      .with('trackerScore', () => ({
        title: 'Tracker Score',
        description: 'We found few/no trackers on the page',
      }))
      .with('region', () => ({
        title: 'Language',
        description:
          'The page seems to be written in a widely spoken language, or matches the language you have set during search (if any)',
      }))
      .with('queryCentrality', () => ({
        title: 'Query Centrality',
        description: 'Other pages in the result set links to this page',
      }))
      .with('inboundSimilarity', () => ({
        title: 'Liked/Disliked',
        description: 'Another page you have liked/disliked influences the ranking of this page',
      }))
      .with('urlSymbols', () => ({
        title: 'URL Symbols',
        description: 'The URL contains few non-alphanumeric characters',
      }))
      .with('linkDensity', () => ({
        title: 'Link Density',
        description: 'The page has a low number of links compared to the amount of text',
      }))
      .with('combinedText', () => ({
        title: 'Combined Text',
        description: 'An overall score of how well the text on the page matches the query',
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
  <div class="mb-2 mt-1 text-xs">
    <div class="flex flex-wrap gap-x-1 gap-y-1">
      {#each signals.slice(0, 3) as [signal, score]}
        <span
          class="rounded-full bg-base-200 px-2 text-center text-xs text-neutral-focus"
          title="The '{signal.replace(/_/g, '-')}' signal has a score of {(
            score.value * score.coefficient
          ).toFixed(2)}"
        >
          {signal.replace(/_/g, '-')}
        </span>
      {/each}
    </div>
  </div>
</details>
