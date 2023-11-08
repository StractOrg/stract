<script lang="ts">
  import type { PageData } from "./$types";
  export let data: PageData;

  $: showAnnotated = true;
  $: queries = showAnnotated
    ? data.queries
    : data.queries.filter((q) => !q.annotated);
</script>

<div class="flex w-full justify-center">
  <div class="flex max-w-xl grow flex-col space-y-4 pt-2">
    <h1 class="text-center text-3xl">Ranking Annotations</h1>
    <label>
      <input type="checkbox" bind:checked={showAnnotated} class="mr-2" />
      Show annotated queries
    </label>
    <div class="grid grid-rows-1 space-y-2">
      {#each queries as { qid, query, annotated }}
        <a href="/annotate/{qid}">
          <div
            class="{annotated
              ? 'bg-green-100'
              : 'bg-slate-100'} rounded-lg border p-4"
          >
            {query}
          </div>
        </a>
      {/each}
    </div>
  </div>
</div>
