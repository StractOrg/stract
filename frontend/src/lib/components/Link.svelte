<script lang="ts">
  import { page } from '$app/stores';
  import { twMerge } from 'tailwind-merge';

  export let href: string;
  export let label: string | undefined = void 0;
  export let exact: boolean = false;
  export let round = false;

  $: active = exact ? $page.url.pathname == href : $page.url.pathname.startsWith(href);
</script>

<a
  aria-label={label}
  {href}
  class={twMerge(
    'flex justify-center rounded-full transition',
    round ? 'p-2' : 'px-2 py-1',
    'text-gray-500 hover:bg-brand-100 hover:text-brand-600 active:bg-brand-50',
    'dark:hover:bg-brand-900 dark:hover:text-stone-50 dark:active:bg-brand-800',
    active && 'bg-brand-50 text-brand-500 dark:bg-brand-800 dark:text-stone-100',
  )}
>
  <slot />
</a>
