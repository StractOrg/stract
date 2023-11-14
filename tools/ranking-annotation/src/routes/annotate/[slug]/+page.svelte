<script lang="ts">
  import Check from '~icons/heroicons/check-20-solid';
  import Home from '~icons/heroicons/home-20-solid';
  import ArrowLeft from '~icons/heroicons/arrow-left-20-solid';
  import ArrowRight from '~icons/heroicons/arrow-right-20-solid';
  import { dndzone } from 'svelte-dnd-action';
  import type { PageData } from "./$types";
  import type { SearchResult } from '$lib/db';
    import { browser } from '$app/environment';

  $: shownSignals = -1;
  $: searchResults = data.searchResults;

  function updateRanks() {
    searchResults.forEach((res, i) => {
      if (res.annotatedRank != null) {
        res.annotatedRank = i;
      }
    });
  }

  function handleConsider(event: CustomEvent<{ items: SearchResult[] }>) {
    searchResults = event.detail.items;
  }

  async function handleDrop(event: CustomEvent<{ items: SearchResult[] }>) {
    searchResults = event.detail.items;
    updateRanks();
    await save();
  }

  async function save() {
    if (!browser) {
      return;
    }

    await fetch(`/api/annotate/${data.query.qid}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        qid: data.query.qid,
        results: searchResults,
      }),
    })
  }

  $: searchResults && save();

  export let data: PageData;
</script>

<div class="flex flex-col w-full items-center mt-5">
  <div class="max-w-xl flex flex-col space-y-5">
    <div class="flex w-full justify-around">
      <div class="w-8 h-8">
        <a href="/" class="w-full h-full text-sky-300 hover:text-sky-500">
          <Home class="w-full h-full"/>
        </a>
      </div>
      <div class="text-xl">
        <div>
          <b class="font-bold">Query:</b> {data.query.query}
        </div>
      </div>
      <div class="flex">
        <a href="/annotate/{data.previousQuery?.qid}" class="text-orange-300 hover:text-orange-500">
          <ArrowLeft class="w-6 h-6"/>
        </a>
        <a href="/annotate/{data.nextQuery?.qid}" class="text-orange-300 hover:text-orange-500">
          <ArrowRight class="w-6 h-6"/>
        </a>
      </div>
    </div>
    <section use:dndzone={{ items: searchResults, dropTargetStyle: {} }} on:consider={handleConsider} on:finalize={handleDrop} class="flex flex-col space-y-3">
      {#each searchResults as res, i (res.id)}
        <div>
          <div class="relative">
            <button class="absolute left-[-2em] top-3" on:click={() => (res.annotatedRank == null) ? res.annotatedRank = i : res.annotatedRank = null}>
              {#if res.annotatedRank != null}
                <div class="text-green-500">
                  <Check class="w-6 h-6" />
                </div>
              {:else}
                <div class="text-slate-200">
                  <Check class="w-6 h-6" />
                </div>
              {/if}
            </button>
            <div class="py-2 px-2 bg-slate-100 border shadow-sm rounded-lg">
              <div>
                <a href="{res.webpage.url}" target="_blank">{res.webpage.url}</a>
              </div>
              <div class="font-bold">
                <a href="{res.webpage.url}" target="_blank">{res.webpage.title}</a>
              </div>
              <div class="line-clamp-3 text-sm">
                {res.webpage.snippet}
              </div>
              <div class="text-sm mt-2 text-slate-500">
                Host centrality: {res.webpage.rankingSignals["host_centrality"].toFixed(3)}
                Tracker score: {res.webpage.rankingSignals["tracker_score"].toFixed(3)}
              </div>
              <div>
                <button class="text-sm text-slate-500" on:click={() => shownSignals = (shownSignals == i) ? -1 : i}>
                  {shownSignals === i ? "Hide" : "Show"} signals
                </button>
              </div>
              {#if shownSignals === i}
                <div class="flex flex-col">
                  {#each Object.entries(res.webpage.rankingSignals) as [name, value]}
                    <div class="flex flex-col text-sm">
                      <div class="flex flex-row space-x-2">
                        <div class="font-bold">{name}:</div>
                        <div>{value}</div>
                      </div>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>
          </div>
        </div>
      {/each}
    </section>
  </div>
</div>