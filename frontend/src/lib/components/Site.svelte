<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { twMerge } from 'tailwind-merge';

  export let href: string;

  const dispatch = createEventDispatcher<{ delete: null }>();
</script>

<span class="bg-secondary group flex overflow-hidden rounded-lg transition">
  <a
    {href}
    target="_blank"
    class={twMerge(
      'bg-secondary py-2',
      'text-primary-content',
      'hover:bg-secondary-focus transition',
      'pl-3 pr-2',
    )}
  >
    <slot />
  </a>
  <span
    class="noscript:hidden bg-primary-content my-2 w-px transition group-hover:bg-transparent"
  />
  <button
    class={twMerge(
      'bg-secondary cursor-pointer px-2 text-sm',
      'text-primary-content',
      'hover:bg-secondary-focus noscript:hidden transition',
    )}
    on:click={() => dispatch('delete')}
  >
    ×
  </button>
</span>
