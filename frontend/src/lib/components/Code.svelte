<script lang="ts">
  import { page } from '$app/stores';

  export let code: string;

  const component =
    $page.data.globals?.highlightjs?.HighlightAuto ||
    import('svelte-highlight').then(({ HighlightAuto }) => HighlightAuto);
</script>

<div class="code rounded-lg bg-base-200 text-sm text-neutral-focus">
  <div class="overflow-auto px-3 py-2">
    {#await component}
      <pre><code>{code}</code></pre>
    {:then comp}
      <svelte:component this={comp} {code} />
    {/await}
  </div>
</div>

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
