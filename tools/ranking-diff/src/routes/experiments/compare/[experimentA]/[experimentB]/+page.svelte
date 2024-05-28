<script lang="ts">
  import type { PageData } from './$types';
  import BackIcon from '~icons/heroicons/arrow-left';

  export let data: PageData;

  const { experimentA, experimentB, queries } = data;
</script>

<div>
  <a href="/">
    <BackIcon class="h-6 w-6" />
  </a>
</div>

<div class="flex flex-col gap-y-10">
  <h1 class="w-full text-center text-xl">
    Common queries between {experimentA.name} and {experimentB.name}
  </h1>
  <div class="grid grid-cols-5 gap-x-2">
    {#each queries as query}
      <a href="#" class="flex h-24 items-center justify-center rounded bg-slate-200 px-2">
        <h2 class="line-clamp-2 break-all">{query.text}</h2>
      </a>
    {/each}
  </div>
</div>
