<script lang="ts">
  import type { HighlightedSpellCorrection } from '$lib/api';

  export let spellCorrection: HighlightedSpellCorrection;

  let link: HTMLAnchorElement;
  export const open = () => {
    link.click();
  };
  export const hasFocus = () => document.activeElement == link;
</script>

<div>
  Did you mean:{' '}
  <a
    class="font-medium"
    href="/search?q={encodeURIComponent(spellCorrection.raw)}"
    bind:this={link}
  >
    {#each spellCorrection.highlighted as frag}
      {#if frag.kind == 'highlighted'}
        <b><i>{frag.text}</i></b>
      {:else}
        {frag.text}
      {/if}
    {/each}
  </a>
</div>
