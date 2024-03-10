<!-- Template to simplify the logic of result links, checking the results in new tab setting -->

<script lang="ts">
  import { resultsInNewTab } from '$lib/stores';
  import { improvements } from '$lib/improvements';

  export let href: string;
  export let _class: string = '';
  export let title: string | null = null;
  export let resultIndex: number | null = null;
</script>

{#if resultIndex != null}
  <a
    {href}
    class={_class}
    {title}
    use:improvements={resultIndex}
    target={$resultsInNewTab ? '_blank' : null}
    rel={$resultsInNewTab ? 'noopener noreferrer' : null}
  >
    <slot />
  </a>
{:else}
  <a
    {href}
    class={_class}
    {title}
    target={$resultsInNewTab ? '_blank' : null}
    rel={$resultsInNewTab ? 'noopener noreferrer' : null}
  >
    <slot />
  </a>
{/if}
