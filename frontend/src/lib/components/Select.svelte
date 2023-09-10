<script lang="ts">
  import { twMerge } from 'tailwind-merge';

  interface $$Props extends Omit<Partial<HTMLSelectElement>, 'form'> {
    form: string;
    submitOnChange?: boolean;
  }

  export let value: string = '';
  export let form: string | undefined = void 0;
  export let submitOnChange = false;
</script>

<select
  {...$$restProps}
  on:change
  bind:value
  {form}
  on:change={(e) => {
    if (submitOnChange && e?.target && e.target instanceof HTMLSelectElement)
      e.target.form?.submit();
  }}
  class={twMerge('rounded py-0 pl-1 pr-5', $$restProps.className)}
  style="background-color: rgba(0, 0, 0, 0); background-image: url(&quot;data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='currentColor'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M19 9l-7 7-7-7' /%3E%3C/svg%3E&quot;); background-position: right center; background-repeat: no-repeat; text-align: right; cursor: pointer; font-size: 0.9rem; background-size: 1em 1em; border: none; appearance: none;"
>
  <slot />
</select>
