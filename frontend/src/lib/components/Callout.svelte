<script lang="ts">
  import type { ComponentType } from 'svelte';
  import InformationCircle from '~icons/heroicons/information-circle-20-solid';
  import CheckCircle from '~icons/heroicons/check-circle-20-solid';
  import ExclamationCircle from '~icons/heroicons/exclamation-circle-20-solid';
  import Fire from '~icons/heroicons/fire-20-solid';
  import { twJoin } from 'tailwind-merge';

  export let kind: 'info' | 'success' | 'warning' | 'error' | 'neutral';
  export let title: string = kind;

  let icon: null | ComponentType = {
    info: InformationCircle,
    success: CheckCircle,
    warning: ExclamationCircle,
    error: Fire,
    neutral: null,
  }[kind];
  let iconLabel = {
    info: 'Information',
    success: 'Success',
    warning: 'Warning',
    error: 'Error',
    neutral: 'Neutral',
  }[kind];
</script>

<div
  class={twJoin(
    'divide-y rounded p-2 shadow',
    kind == 'info' && 'divide-info-content/20 bg-info text-info-content',
    kind == 'success' && 'divide-success-content/20 bg-success text-success-content',
    kind == 'warning' && 'divide-warning-content/20 bg-warning text-warning-content',
    kind == 'error' && 'divide-error-content/20 bg-error text-error-content',
    kind == 'neutral' && 'divide-base-300 bg-base-200 text-neutral',
  )}
>
  <div>
    <slot name="title">
      <div class="flex items-center space-x-1.5 pb-1 font-bold capitalize tracking-wide">
        <svelte:component this={icon} aria-label={iconLabel} />
        <span>
          {title}
        </span>
        <div class="m-0 flex-1" />
        <slot name="top-right" />
      </div>
    </slot>
  </div>
  <div class="pt-1">
    <slot />
  </div>
</div>
