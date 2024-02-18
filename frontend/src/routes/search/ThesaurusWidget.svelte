<script lang="ts">
  import type { PartOfSpeech, ThesaurusWidget, WordMeaning } from '$lib/api';
  import ChevronDown from '~icons/heroicons/chevron-down';
  import ChevronUp from '~icons/heroicons/chevron-up';
  import { slide } from 'svelte/transition';
  import ThesaurusWidgetMeaning from './ThesaurusWidgetMeaning.svelte';

  export let widget: ThesaurusWidget;

  // convert meanings into a list of either pos or meanings
  enum MeaningType {
    pos,
    meaning,
  }
  interface Pos {
    type: MeaningType.pos;
    pos: PartOfSpeech;
  }

  interface Meaning {
    type: MeaningType.meaning;
    meaning: WordMeaning;
  }

  type MeaningOrPos = Pos | Meaning;

  $: meanings = widget.meanings.flatMap((meaning) => {
    return [
      { type: MeaningType.pos, pos: meaning.pos },
      ...(meaning.meanings.map((meaning) => ({
        type: MeaningType.meaning,
        meaning: meaning,
      })) as MeaningOrPos[]),
    ];
  }) as MeaningOrPos[];

  $: nonExpandedMeanings = meanings.slice(0, 2);
  $: expandedMeanings = meanings.slice(2);

  $: expanded = false;

  const posName = (pos: PartOfSpeech): String => {
    switch (pos) {
      case 'noun':
        return 'Noun';
      case 'verb':
        return 'Verb';
      case 'adjective':
        return 'Adjective';
      case 'adjectiveSatellite':
        return 'Adjective Satellite';
      case 'adverb':
        return 'Adverb';
    }
  };
</script>

<div class="rounded-xl border pb-1 pl-5 pr-3 pt-5">
  <div class="text-neutral-focus">
    <h2 class="text-2xl font-bold">{widget.term}</h2>
    <div class="flex flex-col space-y-3 transition">
      {#each nonExpandedMeanings as m}
        {#if m.type == MeaningType.pos}
          <div class="text-sm italic">
            {posName(m.pos)}
          </div>
        {:else if m.type == MeaningType.meaning}
          <ThesaurusWidgetMeaning meaning={m.meaning} />
        {/if}
      {/each}
      {#if expanded}
        <div transition:slide={{ duration: 200 }} class="space-y-3">
          {#each expandedMeanings as m}
            {#if m.type == MeaningType.pos}
              <div class="text-sm italic">
                {posName(m.pos)}
              </div>
            {:else if m.type == MeaningType.meaning}
              <ThesaurusWidgetMeaning meaning={m.meaning} />
            {/if}
          {/each}
        </div>
      {/if}
    </div>
    {#if expandedMeanings.length > 0}
      <button
        class="h-6 w-6 cursor-pointer rounded-full text-primary"
        aria-label={expanded ? 'Show less word definitions' : 'Show more word definitions'}
        on:click={() => (expanded = !expanded)}
      >
        {#if expanded}
          <ChevronUp />
        {:else}
          <ChevronDown />
        {/if}
      </button>
    {/if}
    <div class="float-right mt-1 text-xs italic text-neutral">
      Data from <a href="https://en-word.net/" class="hover:underline">Open English WordNet</a> and
      <a href="https://wordnet.princeton.edu/" class="hover:underline">Princeton WordNet</a>
    </div>
  </div>
</div>
