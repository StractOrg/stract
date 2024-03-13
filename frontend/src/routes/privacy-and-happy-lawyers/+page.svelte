<script lang="ts">
  import Article from '$lib/components/Article.svelte';
  import type { PageData } from './$types';

  export let data: PageData;
</script>

<Article>
  <!-- eslint-disable-next-line svelte/no-at-html-tags -->
  {@html data.md}
</Article>
