<script lang="ts">
  import { themeStore } from '$lib/stores';
  import { THEMES } from '$lib/themes';
</script>

<div class="noscript:hidden">
  <select
    value={$themeStore ?? void 0}
    class="border-none bg-transparent py-0 text-right text-sm text-neutral-focus"
    on:change={(e) => {
      if (!(e.target instanceof HTMLSelectElement)) return;
      $themeStore = e.target.value;
    }}
  >
    <option>Auto</option>
    {#each THEMES as theme}
      <option value={theme.class} class={theme.class}>{theme.name}</option>
    {/each}
  </select>
</div>
