<script lang="ts">
  import { twMerge } from 'tailwind-merge';

  type Kind =
    | 'primary'
    | 'seconday'
    | 'accent'
    | 'neutral'
    | 'info'
    | 'success'
    | 'warning'
    | 'error';

  export let title: string | undefined = void 0;
  export let form: string | undefined = void 0;
  export let type: 'button' | 'submit' | 'reset' | null | undefined = void 0;

  export let pale: boolean = false;
  export let padding: boolean = true;
  export let kind: Kind = 'primary';
  export let _class: string = '';
</script>

<button
  on:click
  {title}
  {form}
  {type}
  class={twMerge(
    'rounded-full py-2 transition active:scale-[98%]',
    padding ? 'px-4' : 'px-2',
    'border border-transparent',
    pale
      ? [
          kind == 'primary' && [
            'text-primary hover:text-primary-focus',
            'bg-transparent hover:bg-primary/5 active:bg-primary/20',
            'border-primary/20 hover:border-primary active:border-primary',
          ],
          kind == 'seconday' && [
            'text-secondary hover:text-secondary-focus',
            'bg-transparent hover:bg-secondary/5 active:bg-secondary/20',
            'border-secondary/20 hover:border-secondary active:border-secondary',
          ],
          kind == 'accent' && [
            'text-accent hover:text-accent-focus',
            'bg-transparent hover:bg-accent/5 active:bg-accent/20',
            'border-accent/20 hover:border-accent active:border-accent',
          ],
          kind == 'neutral' && [
            'text-neutral hover:text-neutral-focus',
            'bg-transparent hover:bg-neutral/5 active:bg-neutral/20',
            'border-neutral/20 hover:border-neutral active:border-neutral',
          ],
          kind == 'info' && [
            'text-info hover:text-info-focus',
            'bg-transparent hover:bg-info/5 active:bg-info/20',
            'border-info/20 hover:border-info active:border-info',
          ],
          kind == 'success' && [
            'text-success hover:text-success-focus',
            'bg-transparent hover:bg-success/5 active:bg-success/20',
            'border-success/20 hover:border-success active:border-success',
          ],
          kind == 'warning' && [
            'text-warning hover:text-warning-focus',
            'bg-transparent hover:bg-warning/5 active:bg-warning/20',
            'border-warning/20 hover:border-warning active:border-warning',
          ],
          kind == 'error' && [
            'text-error hover:text-error-focus',
            'bg-transparent hover:bg-error/5 active:bg-error/20',
            'border-error/20 hover:border-error active:border-error',
          ],
        ]
      : [
          kind == 'primary' && 'bg-primary text-primary-content hover:bg-primary-focus',
          kind == 'seconday' && 'bg-secondary text-secondary-content hover:bg-secondary-focus',
          kind == 'accent' && 'bg-accent text-accent-content hover:bg-accent-focus',
          kind == 'neutral' && 'bg-neutral text-neutral-content hover:bg-neutral-focus',
          kind == 'info' && 'bg-info text-info-content hover:bg-info-focus',
          kind == 'success' && 'bg-success text-success-content hover:bg-success-focus',
          kind == 'warning' && 'bg-warning text-warning-content hover:bg-warning-focus',
          kind == 'error' && 'bg-error text-error-content hover:bg-error-focus',
        ],
        _class,
  )}
>
  <slot />
</button>
