<script lang="ts">
  import ThemeSelect from '$lib/components/ThemeSelect.svelte';
  import PostSearchSelect from './PostSearchSelect.svelte';
  import SafeSearchSelect from './SafeSearchSelect.svelte';
  import MarkPagesWithAdsSelect from './MarkPagesWithAdsSelect.svelte';
  import MarkPagesWithPaywallSelect from './MarkPagesWithPaywallSelect.svelte';
  import ResultsInNewTabs from './ResultsInNewTabs.svelte';
  import UseKeyboardShortcuts from './UseKeyboardShortcuts.svelte';

  const settings = [
    {
      title: 'Safe Search',
      description: 'Remove explicit content from search results',
      type: 'safe-search',
    },
    {
      title: 'Theme',
      description: 'Pick a color scheme',
      type: 'theme',
    },
    {
      title: 'POST Search Requests',
      description: 'Send search queries using POST instead of GET',
      type: 'post',
    },
    {
      title: 'Mark results that contains ads',
      description: 'Add a marker to search results where it is likely that the page contains ads',
      type: 'mark-pages-with-ads',
    },
    {
      title: 'Mark results that contains paywall',
      description:
        'Add a marker to search results where it is likely that the page contains a paywall',
      type: 'mark-pages-with-paywall',
    },
    {
      title: 'Open results in a new tab',
      description: 'Keep the search results window open and have links open in new tabs',
      type: 'results-in-new-tabs',
    },
    {
      title: 'Keyboard shortcuts',
      description: 'Enables the use of keyboard shortcuts on the site',
      type: 'use-keeb-shortcuts',
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
              {#if setting.type == 'safe-search'}
                <SafeSearchSelect />
              {:else if setting.type == 'theme'}
                <ThemeSelect />
              {:else if setting.type == 'post'}
                <PostSearchSelect />
              {:else if setting.type == 'mark-pages-with-ads'}
                <MarkPagesWithAdsSelect />
              {:else if setting.type == 'mark-pages-with-paywall'}
                <MarkPagesWithPaywallSelect />
              {:else if setting.type == 'results-in-new-tabs'}
                <ResultsInNewTabs />
              {:else if setting.type == 'use-keeb-shortcuts'}
                <UseKeyboardShortcuts />
              {/if}
            </div>
          </div>
        </div>
      {/each}
    </div>
  </div>
</div>
