<script lang="ts">
  import ChevronDown from '~icons/heroicons/chevron-down';
  import ChatBubbleLeftRight from '~icons/heroicons/chat-bubble-left-right-20-solid';
  import type { DisplayedWebpage } from '$lib/api';
  import TextSnippet from '$lib/components/TextSnippet.svelte';
  import Button from '$lib/components/Button.svelte';

  export let discussions: DisplayedWebpage[] | undefined;

  let showMore = false;

  $: {
    // let medianScore = 0;
    // if (discussions) {
    //   const scores = discussions.map((d) => d.score || 0);
    //   scores.sort((a, b) => a - b);
    //   const mid = Math.floor(scores.length / 2);
    //   medianScore = scores.length % 2 !== 0 ? scores[mid] : (scores[mid - 1] + scores[mid]) / 2;
    // }

    // if (medianScore < 0.1) {
    //   discussions = undefined;
    // }

    let numDiscussions = discussions?.length || 0;
    if (numDiscussions < 5) {
      discussions = undefined;
    }
  }

  $: shownDiscussions = showMore ? discussions : discussions?.slice(0, 4);
</script>

{#if shownDiscussions && shownDiscussions.length > 0}
  <div class="row-start-5 flex flex-col space-y-1.5 overflow-hidden">
    <div class="flex items-center space-x-1 text-lg">
      <ChatBubbleLeftRight class="text-sm text-neutral" />
      <span>Discussions</span>
    </div>
    <div class="flex flex-col">
      {#each shownDiscussions as discussion}
        <div class="overflow-hidden">
          <div>
            <a class="text-sm text-neutral-focus" href={discussion.url}>
              {discussion.domain}
            </a>
          </div>
          <details class="group">
            <summary class="flex cursor-pointer list-none items-center space-x-2">
              <a
                class="text-md inline-block max-w-[calc(100%-10px)] truncate font-medium text-neutral-focus group-open:underline"
                title={discussion.title}
                href={discussion.url}
              >
                {discussion.title}
              </a>
              <span>
                <ChevronDown class="text-sm transition group-open:rotate-180" />
              </span>
            </summary>

            <div class="mb-3 text-sm font-normal text-neutral-focus">
              {#if typeof discussion.snippet.date == 'string'}
                <span class="text-neutral">{discussion.snippet.date}</span>
              {/if}
              <span class="[&:nth-child(2)]:before:content-['—']">
                <TextSnippet snippet={discussion.snippet.text} />
              </span>
            </div>
          </details>
        </div>
      {/each}
      <div class="noscript:hidden mt-2">
        <Button _class="py-1" kind="neutral" pale on:click={() => (showMore = !showMore)}
          >{showMore ? 'Show less' : 'Show more'}</Button
        >
      </div>
    </div>
  </div>

  <style>
    /* hide marker in safari */
    summary::-webkit-details-marker {
      display: none;
    }
  </style>
{/if}
