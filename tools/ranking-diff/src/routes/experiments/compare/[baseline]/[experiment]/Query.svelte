<script lang="ts">
  import type { Query, Experiment, LikedState } from '$lib';
  import { likedState as getLikedState } from '$lib/api';
  import { onMount } from 'svelte';

  export let baseline: Experiment;
  export let experiment: Experiment;
  export let query: Query;

  let likedState: LikedState = 'none';

  onMount(async () => {
    likedState = await getLikedState(baseline.id, experiment.id, query.id);
  });

  $: color = 'bg-slate-200';
  $: hoverColor = 'hover:bg-slate-300';
  $: {
    if (likedState === 'baseline') {
      color = 'bg-red-200';
      hoverColor = 'hover:bg-red-300';
    } else if (likedState === 'experiment') {
      color = 'bg-green-200';
      hoverColor = 'hover:bg-green-300';
    }
  }
</script>

<a
  href="/experiments/compare/{baseline.id}/{experiment.id}/{query.id}"
  class="flex h-24 items-center justify-center rounded {color} {hoverColor} px-2"
>
  <h2 class="line-clamp-2 break-all">{query.text}</h2>
</a>
