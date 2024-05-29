<script lang="ts">
  import type { PageData } from './$types';
  import BackIcon from '~icons/heroicons/arrow-left';
  import Serp from './Serp.svelte';
  export let data: PageData;

  const { baseline, experiment, query } = data;
</script>

<div>
  <a href="/experiments/compare/{baseline.experiment.id}/{experiment.experiment.id}">
    <BackIcon class="h-6 w-6" />
  </a>
</div>
<div>
  <h1 class="grow text-center text-lg font-semibold">{query.text}</h1>
</div>

<div class="mt-10 flex h-3/4 gap-x-2 overflow-auto">
  <Serp webpages={baseline.serp} />
  <Serp webpages={experiment.serp} />
</div>
