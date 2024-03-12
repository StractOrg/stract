<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { twMerge } from 'tailwind-merge';

  export let href: string;

  const dispatch = createEventDispatcher<{ delete: null }>();
</script>

<span class="group flex overflow-hidden rounded-lg bg-secondary transition">
  <a
    {href}
    target="_blank"
    class={twMerge(
      'bg-secondary py-2',
      'text-primary-content',
      'transition hover:bg-secondary-focus',
      'pl-3 pr-2',
    )}
  >
    <slot />
  </a>
  <span
    class="noscript:hidden my-2 w-px bg-primary-content transition group-hover:bg-transparent"
  />
  <button
    class={twMerge(
      'cursor-pointer bg-secondary px-2 text-sm',
      'text-primary-content',
      'noscript:hidden transition hover:bg-secondary-focus',
    )}
    on:click={() => dispatch('delete')}
  >
    ×
  </button>
</span>
