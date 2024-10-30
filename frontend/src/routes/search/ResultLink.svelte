<script lang="ts">
  import { resultsInNewTab } from '$lib/stores';
  import { improvements } from '$lib/improvements';

  export let href: string;
  export let _class: string = '';
  export let title: string | null = null;
  export let resultIndex: number | null = null;

  let link: HTMLAnchorElement;
  export const focus = () => {
    link.focus();
  };
  export const clearFocus = () => {
    link.blur();
  };
  export const open = () => {
    link.click();
  };
  export const openInCurrentTab = () => {
    window.location.href = link.href;
  };
  export const openInNewTab = () => {
    window.open(link.href, '_blank', 'noopener');
  };

  export const hasFocus = () => document.activeElement == link;

  export const getUrl = () => new URL(link.href);
</script>

{#if resultIndex != null}
  <a
    {href}
    class={_class}
    {title}
    use:improvements={resultIndex}
    target={$resultsInNewTab ? '_blank' : null}
    rel="noopener nofollow"
    bind:this={link}
  >
    <slot />
  </a>
{:else}
  <a
    {href}
    class={_class}
    {title}
    target={$resultsInNewTab ? '_blank' : null}
    rel="noopener nofollow"
    bind:this={link}
  >
    <slot />
  </a>
{/if}
