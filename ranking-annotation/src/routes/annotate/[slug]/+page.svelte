<script lang="ts">
  import type { PageData } from "./$types";
  export let data: PageData;
</script>

<div class="flex flex-col w-full items-center">
  <div class="max-w-xl flex flex-col space-y-5">
    <div>
      <b class="font-bold">Query:</b> {data.query.query}
    </div>

    <div class="flex flex-col space-y-3">
      {#each data.searchResults as res}
        <div class="py-2 px-2 border shadow-sm rounded-lg">
          <div>
            <a href="{res.webpage.url}">{res.webpage.url}</a>
          </div>
          <div>
            <a href="{res.webpage.url}">{res.webpage.title}</a>
          </div>
          <div class="line-clamp-3 text-sm">
            {res.webpage.snippet}
          </div>
        </div>
      {/each}
    </div>
  </div>
</div>