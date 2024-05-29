<script lang="ts">
  import type { Query, Experiment } from '$lib';

  export let baseline: Experiment;
  export let experiment: Experiment;
  export let query: Query;
</script>

<a
  href="/experiments/compare/{baseline.id}/{experiment.id}/{query.id}"
  class="flex h-24 items-center justify-center rounded bg-slate-200 px-2 hover:bg-slate-300"
>
  <h2 class="line-clamp-2 break-all">{query.text}</h2>
</a>
