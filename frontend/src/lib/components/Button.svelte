<script lang="ts">
  import { twMerge } from 'tailwind-merge';

  type Kind = 'brand' | 'amber' | 'red';

  export let title: string | undefined = void 0;
  export let form: string | undefined = void 0;
  export let type: 'button' | 'submit' | 'reset' | null | undefined = void 0;

  export let pale: boolean = false;
  export let padding: boolean = true;
  export let kind: Kind = 'brand';
</script>

<button
  on:click
  {title}
  {form}
  {type}
  class={twMerge(
    'noscript:hidden rounded-full py-2 transition',
    padding ? 'px-4' : 'px-2',
    'text-white dark:text-white',
    'border border-transparent',
    pale
      ? [
          kind == 'brand' && [
            'text-brand-900 dark:text-brand-50',
            'bg-white hover:bg-brand-50 active:bg-brand-100',
            'dark:bg-transparent dark:hover:bg-brand-800 dark:active:bg-brand-700',
            'border-brand-100 hover:border-brand-200 active:border-brand-300',
            'dark:border-brand-600 dark:hover:border-brand-500 dark:active:border-brand-400',
          ],
          kind == 'amber' && [
            'text-amber-900 dark:text-amber-50',
            'bg-white hover:bg-amber-50 active:bg-amber-100',
            'dark:bg-transparent dark:hover:bg-amber-800 dark:active:bg-amber-700',
            'border-amber-100 hover:border-amber-200 active:border-amber-300',
            'dark:border-amber-600 dark:hover:border-amber-500 dark:active:border-amber-400',
          ],
          kind == 'red' && [
            'text-red-900 dark:text-red-50',
            'bg-white hover:bg-red-50 active:bg-red-100',
            'dark:bg-transparent dark:hover:bg-red-800 dark:active:bg-red-700',
            'border-red-100 hover:border-red-200 active:border-red-300',
            'dark:border-red-600 dark:hover:border-red-500 dark:active:border-red-400',
          ],
        ]
      : [
          kind == 'brand' && [
            'bg-brand-500 hover:bg-brand-600 active:bg-brand-700',
            'dark:bg-brand-800 dark:hover:bg-brand-700 dark:active:bg-brand-600',
          ],
          kind == 'amber' && [
            'bg-amber-500 hover:bg-amber-600 active:bg-amber-700',
            'dark:bg-amber-800 dark:hover:bg-amber-700 dark:active:bg-amber-600',
          ],
          kind == 'red' && [
            'bg-red-500 hover:bg-red-600 active:bg-red-700',
            'dark:bg-red-800 dark:hover:bg-red-700 dark:active:bg-red-600',
          ],
        ],
  )}
>
  <slot />
</button>
