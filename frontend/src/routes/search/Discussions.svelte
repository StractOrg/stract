<script lang="ts">
  import ChevronDown from '~icons/heroicons/chevron-down';
  import ChatBubbleLeftRight from '~icons/heroicons/chat-bubble-left-right';
  import type { Webpage } from '$lib/api';

  export let discussions: Webpage[];

  let showMore = false;

  $: shownDiscussions = showMore ? discussions : discussions.slice(0, 4);
</script>

<div class="flex flex-col space-y-1.5 overflow-hidden">
  <div class="flex items-center space-x-1 text-lg">
    <ChatBubbleLeftRight class="text-sm" />
    <span>Discussions</span>
  </div>
  <div class="flex flex-col">
    {#each shownDiscussions as discussion}
      <div class="overflow-hidden">
        <div>
          <a class="text-sm" href={discussion.url}>
            {discussion.domain}
          </a>
        </div>
        <details class="group">
          <summary class="flex cursor-pointer list-none items-center space-x-2">
            <a
              class="text-md inline-block max-w-[calc(100%-10px)] truncate font-medium group-open:underline"
              title={discussion.title}
              href={discussion.url}
            >
              {discussion.title}
            </a>
            <ChevronDown class="text-sm transition group-open:rotate-180" />
          </summary>

          {#if discussion.snippet.type == 'normal'}
            <div class="mb-3 text-sm font-normal text-snippet">
              {#if typeof discussion.snippet.date == 'string'}
                <span class="text-gray-500">{discussion.snippet.date}</span>
              {/if}
              <span class="[&:nth-child(2)]:before:content-['—']">
                {@html discussion.snippet.text}
              </span>
            </div>
          {/if}
        </details>
      </div>
    {/each}
    <button
      class="noscript:hidden mt-3 w-fit rounded-full border px-2 py-1 hover:cursor-pointer hover:bg-neutral-100"
      on:click={() => (showMore = !showMore)}
    >
      {showMore ? 'Show less' : 'Show more'}
    </button>
  </div>
</div>
