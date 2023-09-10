<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { twMerge } from 'tailwind-merge';

  export let href: string;

  const dispatch = createEventDispatcher<{ delete: null }>();
</script>

<span class="group flex overflow-hidden rounded-lg bg-brand-500 transition dark:bg-brand-800">
  <a
    {href}
    target="_blank"
    class={twMerge(
      'bg-brand-500 py-2',
      'text-white',
      'transition hover:bg-brand-600 active:bg-brand-700',
      'dark:bg-brand-800 dark:hover:bg-brand-700 dark:active:bg-brand-600',
      'pl-3 pr-2',
    )}
  >
    <slot />
  </a>
  <span class="my-2 w-px bg-brand-100 transition group-hover:bg-transparent dark:bg-brand-950" />
  <button
    class={twMerge(`
          remove-site cursor-pointer bg-brand-500 px-2 text-sm
        text-white
        transition hover:bg-brand-600 active:bg-brand-700
          dark:bg-brand-800 dark:hover:bg-brand-700 dark:active:bg-brand-600
        `)}
    on:click={() => dispatch('delete')}
  >
    ×
  </button>
</span>
