<script lang="ts">
  import ThemeSelect from '$lib/components/ThemeSelect.svelte';
  import PostSearchSelect from './PostSearchSelect.svelte';
  import SafeSearchSelect from './SafeSearchSelect.svelte';
  import MarkPagesWithAdsSelect from './MarkPagesWithAdsSelect.svelte';
  import MarkPagesWithPaywallSelect from './MarkPagesWithPaywallSelect.svelte';
  import ResultsInNewTabs from './ResultsInNewTabs.svelte';
  import UseKeyboardShortcuts from './UseKeyboardShortcuts.svelte';
  import ShowRankingSignals from './ShowRankingSignals.svelte';

  const settings = [
    {
      title: 'Safe Search',
      description: 'Remove explicit content from search results',
      _type: 'safe-search',
    },
    {
      title: 'Theme',
      description: 'Pick a color scheme',
      _type: 'theme',
    },
    {
      title: 'POST Search Requests',
      description: 'Send search queries using POST instead of GET',
      _type: 'post',
    },
    {
      title: 'Mark results that contains ads',
      description: 'Add a marker to search results where it is likely that the page contains ads',
      _type: 'mark-pages-with-ads',
    },
    {
      title: 'Mark results that contains paywall',
      description:
        'Add a marker to search results where it is likely that the page contains a paywall',
      _type: 'mark-pages-with-paywall',
    },
    {
      title: 'Open results in a new tab',
      description: 'Keep the search results window open and have links open in new tabs',
      _type: 'results-in-new-tabs',
    },
    {
      title: 'Keyboard shortcuts',
      description: 'Enables the use of keyboard shortcuts on the site',
      _type: 'use-keeb-shortcuts',
    },
    {
      title: 'Show ranking signals',
      description: 'Show the top ranking signals for search result',
      _type: 'show-ranking-signals',
    },
  ] as const;
</script>

<div class="[&_a]:font-medium [&_a]:underline">
  <div class="space-y-3">
    <h1 class="text-2xl font-medium">Preferences</h1>
    <div class="flex flex-col space-y-5">
      {#each settings as setting}
        <div class="flex flex-col pr-1 sm:flex-row sm:justify-between">
          <div class="flex flex-col pr-5">
            <h2 class="text-lg">{setting.title}</h2>
            <p class="text-sm">{setting.description}</p>
          </div>
          <div class="flex flex-col pr-5 sm:flex-row sm:justify-between sm:space-y-0">
            <div>
              {#if setting._type == 'safe-search'}
                <SafeSearchSelect />
              {:else if setting._type == 'theme'}
                <ThemeSelect />
              {:else if setting._type == 'post'}
                <PostSearchSelect />
              {:else if setting._type == 'mark-pages-with-ads'}
                <MarkPagesWithAdsSelect />
              {:else if setting._type == 'mark-pages-with-paywall'}
                <MarkPagesWithPaywallSelect />
              {:else if setting._type == 'results-in-new-tabs'}
                <ResultsInNewTabs />
              {:else if setting._type == 'use-keeb-shortcuts'}
                <UseKeyboardShortcuts />
              {:else if setting._type == 'show-ranking-signals'}
                <ShowRankingSignals />
              {/if}
            </div>
          </div>
        </div>
      {/each}
    </div>
  </div>
</div>
