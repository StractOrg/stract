
<script lang="ts">
  import type { StackOverflowQuestion, StackOverflowAnswer } from "$lib/api";
  import Code from '$lib/components/Code.svelte';
  import HandThumbUp from '~icons/heroicons/hand-thumb-up';
  import Check from '~icons/heroicons/check';

    export let question: StackOverflowQuestion;
    export let answers: StackOverflowAnswer[];
</script>
<div class="line-clamp-2">
{#each question.body as passage}
    {#if passage.type == "text"}
    {passage.value}
    {/if}
{/each}
</div>
<div class="flex space-x-4 pt-2">
    {#each answers.slice(0, 3) as answer}
    <div class="w-1/3 overflow-hidden">
    <a class="block h-56 hover:bg-base-200/80 p-2 border overflow-hidden rounded-lg" href={answer.url}>
        <div class="flex w-full items-center justify-between space-x-1 mb-1 text-neutral text-xs">
            <div class="flex">
                {answer.date}
            </div>
            <div class="flex space-x-1">
                <span class="h-fit">
                {answer.upvotes}
                </span>
                <div class="h-fit">
                <HandThumbUp class="w-4" />
                </div>
                {#if answer.accepted}
                <div class="h-fit text-green-600">
                    <Check class="w-4" />
                </div>
                {/if}
            </div>
        </div>
        <div>
        {#each answer.body as passage}
        {#if passage.type == "text"}
            {passage.value}
        {:else if passage.type == "code"}
            <Code code={passage.value} transparentBackground={true} />
        {/if}
        {/each}
        </div>
    </a>
    </div>
    {/each}
</div>