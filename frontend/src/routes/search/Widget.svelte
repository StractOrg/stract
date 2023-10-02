<script lang="ts">
  import type { Widget } from '$lib/api';
  import ThesaurusWidget from './ThesaurusWidget.svelte';
  import Calculator from '$lib/widgets/Calculator.svelte';

  export let widget: Widget;
</script>

{#if widget.type == 'calculator'}
  <Calculator calc={widget.value} />
{:else if widget.type == 'thesaurus'}
  <ThesaurusWidget widget={widget.value} />
{/if}
