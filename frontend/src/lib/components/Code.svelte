<script lang="ts">
  import { page } from '$app/stores';

  export let code: string;

  const component =
    $page.data.globals?.highlightjs?.HighlightAuto ||
    import('svelte-highlight').then(({ HighlightAuto }) => HighlightAuto);
</script>

<div
  class="code rounded-lg bg-slate-50 text-sm text-gray-600 dark:bg-stone-800 dark:text-brand-200"
>
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
    @apply text-brand-600 dark:text-brand-400;
  }
  .code :global(.hljs-number) {
    @apply text-teal-700 dark:text-teal-300;
  }
  .code :global(.hljs-literal) {
    @apply text-teal-700 dark:text-teal-300;
  }
  .code :global(.hljs-string) {
    @apply text-green-700 dark:text-green-300;
  }
  .code :global(.hljs-comment) {
    @apply text-teal-800 dark:text-teal-200/50;
  }
  .code :global(.hljs-title) {
    @apply text-emerald-600 dark:text-emerald-300;
  }
</style>
