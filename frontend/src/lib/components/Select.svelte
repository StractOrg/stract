<script lang="ts" generics="T extends number | string">
  interface $$Props extends Omit<Partial<HTMLSelectElement>, 'form' | 'value'> {
    class?: string;
    value: T;
    form?: string;
    submitOnChange?: boolean;
  }

  export let value: T;
  export let form: string | undefined = void 0;
  export let submitOnChange = false;
</script>

<div>
  <select
    {...$$restProps}
    on:change
    bind:value
    {form}
    on:change={(e) => {
      if (submitOnChange && e?.target && e.target instanceof HTMLSelectElement)
        e.target.form?.submit();
    }}
  >
    <slot />
  </select>
</div>

<style lang="postcss">
  select {
    @apply rounded border-none bg-transparent py-0 text-right;
  }
</style>
