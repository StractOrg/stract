<script lang="ts">
  import { getButtonTailwindStyle, type ButtonKind } from '$lib/themes';

  export let title: string | undefined = void 0;
  export let form: string | undefined = void 0;
  export let type: 'button' | 'submit' | 'reset' | null | undefined = void 0;

  export let pale: boolean = false;
  export let padding: boolean = true;
  export let kind: ButtonKind = 'primary';
  export let _class: string = '';
  export let aria_label: string | undefined = void 0;
</script>

<button
  on:click
  {title}
  {form}
  {type}
  class={getButtonTailwindStyle(padding, pale, kind, _class)}
  aria-label={aria_label}
>
  <slot />
</button>
