<script lang="ts">
  import { HighlightAuto } from 'svelte-highlight';

  export let code: string;
  export let transparentBackground = false;

  $: codeHasNewlines = code.includes('\n');
</script>

{#if codeHasNewlines}
  <div
    class="code rounded-lg {transparentBackground ? '' : 'bg-base-200'} text-sm text-neutral-focus"
  >
    <div class="overflow-auto px-3 py-2">
      <HighlightAuto {code} />
    </div>
  </div>
{:else}
  <span
    class="code rounded-lg {transparentBackground ? '' : 'bg-base-200'} text-sm text-neutral-focus"
  >
    <span class="overflow-auto px-3 py-2">
      <code>{code}</code>
    </span>
  </span>
{/if}

<style lang="postcss">
  .code :global(.hljs-keyword) {
    @apply text-primary-focus;
  }
  .code :global(.hljs-number) {
    @apply text-secondary;
  }
  .code :global(.hljs-literal) {
    @apply text-secondary;
  }
  .code :global(.hljs-string) {
    @apply text-accent-focus;
  }
  .code :global(.hljs-comment) {
    @apply text-neutral/80;
  }
  .code :global(.hljs-title) {
    @apply text-secondary-focus;
  }
</style>
