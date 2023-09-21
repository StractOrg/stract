<script lang="ts">
  import ThemeSelect from '$lib/components/ThemeSelect.svelte';
  import PostSearchSelect from '$lib/components/PostSearchSelect.svelte';
  import { safeSearchStore } from '$lib/stores';

  const safeSearchOptions = [
    ['on', true],
    ['off', false],
  ] as const;

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
    }
  ] as const;
</script>

<div class="[&_a]:font-medium [&_a]:underline">
  <div class="space-y-3">
    <h1 class="text-2xl font-medium">Preferences</h1>
    <div class="flex flex-col space-y-5">
      {#each settings as setting}
        <div class="flex flex-col sm:flex-row sm:justify-between pr-1">
          <div class="flex flex-col">
            <h2 class="text-lg">{setting.title}</h2>
            <p>{setting.description}</p>
          </div>
          <div class="flex flex-col pr-5 sm:flex-row sm:justify-between sm:space-y-0">
            <div>
              {#if setting.type == 'safe-search'}
                <div class="flex space-x-4 pr-2">
                  {#each safeSearchOptions as [name, state]}
                    <label class="flex cursor-pointer flex-col items-center" for="safe-search-{state}">
                      <input
                        type="radio"
                        name="safe-search"
                        id="safe-search-{state}"
                        value={state}
                        bind:group={$safeSearchStore}
                      />
                      <span class="capitalize">{name}</span>
                    </label>
                  {/each}
                </div>
              {:else if setting.type == 'theme'}
                <ThemeSelect />
              {:else if setting.type == "post"}
                <PostSearchSelect />
              {/if}
            </div>
          </div>
        </div>
      {/each}
    </div>
  </div>
</div>
