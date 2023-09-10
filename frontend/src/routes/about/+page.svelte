<script lang="ts">
  import Article from '$lib/components/Article.svelte';
  import type { PageData } from './$types';

  export let data: PageData;
</script>

<Article>
  {@html data.md}
</Article>
