<script lang="ts">
  import type { EntitySnippet } from '$lib/api';

  export let snippet: EntitySnippet;
</script>

{#each snippet.fragments as fragment}
  {#if fragment.kind == 'normal'}
    {fragment.text}
  {:else if fragment.kind == 'link'}
    {' '}<a href={fragment.href} class="hover:underline">{fragment.text.replaceAll('*', '•')}</a>
  {/if}
{/each}
