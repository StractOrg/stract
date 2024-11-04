<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { twMerge } from 'tailwind-merge';

  export let href: string;

  const dispatch = createEventDispatcher<{ delete: null }>();
</script>

<span class="group flex rounded-lg bg-secondary transition">
  <a
    {href}
    target="_blank"
    class={twMerge(
      'py-2',
      'text-primary-content',
      'transition hover:bg-secondary-focus',
      'rounded-l-lg pl-3 pr-2',
    )}
  >
    <slot />
  </a>
  <span class="noscript:hidden my-2 w-px bg-primary-content transition group-hover:bg-transparent"
  ></span>
  <button
    class={twMerge(
      'cursor-pointer text-sm',
      'text-primary-content',
      'noscript:hidden rounded-r-lg transition hover:bg-secondary-focus',
    )}
    on:click|preventDefault={() => dispatch('delete')}
    title="Remove site"
  >
    <span class="px-2"> × </span>
  </button>
</span>
