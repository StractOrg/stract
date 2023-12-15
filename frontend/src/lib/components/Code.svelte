<script lang="ts">
  import { HighlightAuto } from 'svelte-highlight';

  export let code: string;
  export let transparentBackground = false;

  $: isInline = !code.includes('\n');
</script>

{#if isInline}
  <span
    class="code rounded-md {transparentBackground ? '' : 'bg-base-200'} text-neutral-focus inline-block mx-1"
  >
    <span class="overflow-auto px-2">
      <code>{code}</code>
    </span>
  </span>
{:else}
  <div
    class="code rounded-lg {transparentBackground ? '' : 'bg-base-200'} text-neutral-focus my-2"
  >
    <div class="overflow-auto px-3 py-2">
      <HighlightAuto {code} />
    </div>
  </div>
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
