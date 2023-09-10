<script lang="ts">
  import type { DisplayedAnswer } from '$lib/api';

  export let directAnswer: DisplayedAnswer;
</script>

<div class="flex flex-col border-b">
  <div class="answer">{directAnswer.answer}</div>
  <div class="mb-5 text-sm [&>b]:font-bold">{@html directAnswer.snippet}</div>
  <div class="flex flex-col">
    <a
      class="m-0 inline-block max-w-[calc(100%-30px)] truncate p-0 text-sm text-gray-800 hover:no-underline"
      href={directAnswer.url}
    >
      {directAnswer.prettyUrl}
    </a>
    <div class="answer-title">
      <a
        class="inline-block max-w-[calc(100%-30px)] truncate text-xl font-medium text-blue-800 visited:text-purple-800"
        href={directAnswer.url}
      >
        {directAnswer.title}
      </a>
    </div>
  </div>
</div>
