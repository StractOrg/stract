<script lang="ts">
  import ThemeSelect from '$lib/components/ThemeSelect.svelte';
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
  ] as const;
</script>

<div class="[&_a]:font-medium [&_a]:underline">
  <div class="space-y-3">
    <h1 class="text-2xl font-medium">Preferences</h1>
    <div>
      {#each settings as setting}
        <h2 class="text-lg">{setting.title}</h2>
        <div class="flex flex-col space-y-1 pr-5 sm:flex-row sm:justify-between sm:space-y-0">
          <p>{setting.description}</p>
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
          {/if}
        </div>
      {/each}

      <p class="pt-5 text-neutral/50">More to come soon</p>
    </div>
  </div>
</div>
