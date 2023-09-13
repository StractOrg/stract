<script lang="ts" generics="T">
  import { createEventDispatcher } from 'svelte';

  type Option = { value: T; label: string; title?: string };

  interface $$Props extends Omit<Partial<HTMLSelectElement>, 'form' | 'value' | 'options'> {
    class?: string;
    value: T;
    form?: string;
    submitOnChange?: boolean;
    options: Option[];
  }

  export let value: T;
  export let form: string | undefined = void 0;
  export let submitOnChange = false;
  export let options: $$Props['options'];

  const dispatch = createEventDispatcher<{ change: T }>();
</script>

<div>
  <select
    {...$$restProps}
    {form}
    on:change={(e) => {
      if (!e?.target || !(e.target instanceof HTMLSelectElement)) return;

      const chosen = options[e.target.options.selectedIndex];
      value = chosen.value;
      dispatch('change', chosen.value);

      if (submitOnChange) e.target.form?.submit();
    }}
  >
    {#each options as option}
      <option value={option.value} selected={option.value == value} title={option.title}
        >{option.label}</option
      >
    {/each}
  </select>
</div>

<style lang="postcss">
  select {
    @apply rounded border-none bg-transparent py-0 text-right;
  }
</style>
