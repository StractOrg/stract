<script lang="ts">
  import type { TextSnippet } from '$lib/api';

  export let snippet: TextSnippet;
</script>

<p>
  {#each snippet.fragments as fragment}
    {#if fragment.kind == 'normal'}
      {fragment.text}
    {:else if fragment.kind == 'highlighted'}
      <b>{fragment.text}</b>
    {/if}
  {/each}
</p>
