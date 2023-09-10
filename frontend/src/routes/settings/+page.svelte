<script lang="ts">
  import { safeSearchStore } from '$lib/stores';

  const safeSearchOptions = [
    ['on', true],
    ['off', false],
  ] as const;
</script>

<div class="[&_a]:font-medium [&_a]:underline">
  <div class="space-y-3">
    <h1 class="text-2xl font-medium">Preferences</h1>
    <div>
      <h2 class="text-lg">Safe Search</h2>
      <div class="flex flex-col space-y-1 pr-5 sm:flex-row sm:justify-between sm:space-y-0">
        <p>Remove explicit content from search results</p>
        <div class="flex space-x-4">
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
      </div>

      <p class="pt-5 text-neutral-300 dark:text-stone-500">More to come soon</p>
    </div>
  </div>
</div>
