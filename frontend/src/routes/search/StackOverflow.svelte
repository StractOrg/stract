<script lang="ts">
  import HandThumbUp from '~icons/heroicons/hand-thumb-up';
  import type { StackOverflowAnswer } from '$lib/api';
  import Code from '$lib/components/Code.svelte';
  import StackOverflowText from './StackOverflowText.svelte';
  import ResultLink from './ResultLink.svelte';

  export let title: string;
  export let answer: StackOverflowAnswer;
</script>

<div class="flex flex-col space-y-5 overflow-hidden rounded-lg border p-5 md:max-w-lg">
  <div class="flex flex-col space-y-1">
    <div class="flex grow justify-between space-x-2">
      <ResultLink _class="flex text-lg font-medium leading-6" href={answer.url}>
        {title}
      </ResultLink>
      <div class="flex items-center space-x-1">
        <span class="h-fit">
          {answer.upvotes}
        </span>
        <div class="h-fit">
          <HandThumbUp class="w-4" />
        </div>
      </div>
    </div>
    <div class="flex grow justify-between space-x-2 text-sm">
      <div>
        <ResultLink href={answer.url}>
          {answer.url}
        </ResultLink>
      </div>
      <div>{answer.date}</div>
    </div>
  </div>
  <hr class="border-stone-700" />
  <div class="inline-block max-h-96 overflow-y-scroll text-sm">
    {#each answer.body as part}
      {#if part.type == 'code'}
        <Code code={part.value} />
      {:else if part.type == 'text'}
        <StackOverflowText text={part.value} />
      {/if}
    {/each}
  </div>
</div>
