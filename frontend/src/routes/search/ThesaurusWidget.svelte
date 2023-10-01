<script lang="ts">
    import type { ThesaurusWidget } from '$lib/api';

    export let widget: ThesaurusWidget;

    const posName = (pos: "noun" | "verb" | "adjective" | "adjectiveSatellite" | "adverb"): String => {
        switch (pos) {
            case "noun":
                return "Noun";
            case "verb":
                return "Verb";
            case "adjective":
                return "Adjective";
            case "adjectiveSatellite":
                return "Adjective Satellite";
            case "adverb":
                return "Adverb";
        }
    }
</script>
  
<div class="rounded-xl border pl-5 pr-3 pt-5 pb-1">
    <div class="text-neutral-focus">
        <h2 class="font-bold text-2xl">{widget.term}</h2>
        <div class="flex flex-col space-y-2">
            {#each widget.meanings as {pos, meanings}}
                <div>
                    <div class="italic text-sm">
                        {posName(pos)}
                    </div>
                    <ol class="list-decimal space-y-2 pl-5">
                        {#each meanings as meaning}
                            <li>
                                <div>
                                    {meaning.definition}
                                </div>
                                {#if meaning.similar.length > 0}
                                    <div class="flex space-x-1 text-sm">
                                        <div class="text-primary-focus font-medium">
                                            Similar:
                                        </div>
                                        <div class="inline-block space-x-1">
                                        {#each meaning.similar as similar}
                                        <div class="float-left inline [&:not(:last-child)]:after:content-[',']">
                                            <a class="float-left hover:underline" href="/search?q={encodeURIComponent('definition of ' + similar)}">
                                                {similar}
                                            </a>
                                        </div>
                                        {/each}
                                        </div>
                                    </div>
                                {/if}
                            </li>
                        {/each}
                    </ol>
                </div>
            {/each}
        </div>
        <div class="text-[0.75rem] italic text-neutral float-right mt-1">
            Data from <a href="https://en-word.net/" class="hover:underline">Open English WordNet</a> and <a href="https://wordnet.princeton.edu/" class="hover:underline">Princeton WordNet</a>
        </div>
    </div>
</div>