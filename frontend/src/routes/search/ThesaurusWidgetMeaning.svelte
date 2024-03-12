<script lang="ts">
  import type { WordMeaning } from '$lib/api';
  import ResultLink from './ResultLink.svelte';

  export let meaning: WordMeaning;
</script>

<div>
  <div class="text-sm">
    {meaning.definition}
  </div>
  {#if meaning.similar.length > 0}
    <div class="flex space-x-1 text-xs">
      <div class="font-medium text-primary-focus">Similar:</div>
      <div class="inline-block space-x-1">
        {#each meaning.similar as similar}
          <div class="float-left inline [&:not(:last-child)]:after:content-[',']">
            <a
              class="float-left hover:underline"
              href="/search?q={encodeURIComponent('definition of ' + similar)}"
            >
              {similar}
            </a>
          </div>
        {/each}
      </div>
    </div>
  {:else if meaning.examples.length > 0}
    <div class="text-xs text-neutral-focus">
      "{meaning.examples[0]}"
    </div>
  {/if}
</div>
