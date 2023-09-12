<script lang="ts">
  import { page } from '$app/stores';
  import { twMerge } from 'tailwind-merge';

  export let href: string;
  export let label: string | undefined = void 0;
  export let exact: boolean = false;
  export let round = false;
  export let active = false;

  $: isActive =
    active || (exact ? $page.url.pathname == href : $page.url.pathname.startsWith(href));
</script>

<a
  aria-label={label}
  {href}
  class={twMerge(
    'flex justify-center rounded-full transition',
    round ? 'p-2' : 'px-2 py-1',
    'text-neutral-focus hover:bg-primary/10 hover:text-primary-focus active:bg-primary-focus/10',
    isActive && 'bg-primary/20 text-primary-focus',
  )}
>
  <slot />
</a>
