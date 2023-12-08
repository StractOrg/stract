<script lang="ts">
  import HandThumbUp from '~icons/heroicons/hand-thumb-up';
  import type { StackOverflowAnswer } from '$lib/api';
  import Code from '$lib/components/Code.svelte';

  export let title: string;
  export let answer: StackOverflowAnswer;
</script>

<div class="flex flex-col space-y-5 overflow-hidden rounded-lg border p-5 md:max-w-lg">
  <div class="flex flex-col space-y-1">
    <div class="flex grow justify-between space-x-2">
      <a class="flex text-lg font-medium leading-6" href={answer.url}>{title}</a>
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
        <a href={answer.url}>{answer.url}</a>
      </div>
      <div>{answer.date}</div>
    </div>
  </div>
  <hr class="border-stone-700" />
  <div class="flex flex-col space-y-3 text-sm">
    {#each answer.body as part}
      {#if part.type == 'code'}
        <Code code={part.value} />
      {:else if part.type == 'text'}
        <span>{part.value}</span>
      {/if}
    {/each}
  </div>
</div>
