<script lang="ts">
  import type { Widget } from '$lib/api';
  import ThesaurusWidget from './ThesaurusWidget.svelte';

  export let widget: Widget;
</script>

{#if widget._type == 'calculator'}
  <div class="rounded-xl border p-5">
    <div class="flex flex-col items-end">
      <div class="flex w-fit text-xs text-neutral">
        {widget.value.input} =
      </div>
      <div class="flex w-fit text-3xl font-bold">
        {widget.value.result}
      </div>
    </div>
  </div>
{:else if widget._type == 'thesaurus'}
  <ThesaurusWidget widget={widget.value} />
{/if}
