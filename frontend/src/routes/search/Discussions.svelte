<script lang="ts">
  import ChevronDown from '~icons/heroicons/chevron-down';
  import ChatBubbleLeftRight from '~icons/heroicons/chat-bubble-left-right-20-solid';
  import type { DisplayedWebpage } from '$lib/api';
  import TextSnippet from '$lib/components/TextSnippet.svelte';
  import Button from '$lib/components/Button.svelte';

  export let discussions: DisplayedWebpage[];

  let showMore = false;

  $: shownDiscussions = showMore ? discussions : discussions.slice(0, 4);
</script>

<div class="flex flex-col space-y-1.5 overflow-hidden">
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
              class="text-md text-neutral-focus inline-block max-w-[calc(100%-10px)] truncate font-medium group-open:underline"
              title={discussion.title}
              href={discussion.url}
            >
              {discussion.title}
            </a>
            <ChevronDown class="text-sm transition group-open:rotate-180" />
          </summary>

          {#if discussion.snippet.type == 'normal'}
            <div class="mb-3 text-sm font-normal text-neutral-focus">
              {#if typeof discussion.snippet.date == 'string'}
                <span class="text-neutral">{discussion.snippet.date}</span>
              {/if}
              <span class="[&:nth-child(2)]:before:content-['—']">
                <TextSnippet snippet={discussion.snippet.text} />
              </span>
            </div>
          {/if}
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
