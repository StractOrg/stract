<script lang="ts">
  import type { Widget } from '$lib/api';

  export let widget: Widget;
</script>

{#if widget.type == 'calculator'}
  <div class="flex flex-col items-end rounded-xl border p-5">
    <div class="flex w-fit text-xs text-gray-500">
      {widget.value.input} =
    </div>
    <div class="flex w-fit text-3xl font-bold">
      {widget.value.result}
    </div>
  </div>
{/if}
