<script lang="ts">
  import Home from '~icons/heroicons/home-20-solid';
  import ArrowLeft from '~icons/heroicons/arrow-left-20-solid';
  import ArrowRight from '~icons/heroicons/arrow-right-20-solid';
  import type { PageData } from "./$types";
  import { browser } from '$app/environment';

  $: shownSignals = -1;
  $: searchResults = data.searchResults;
  $: selected = 0;

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

  function handleKeyPress(event: KeyboardEvent) {
    if (event.key === "ArrowUp") {
      event.preventDefault();
      selected = Math.max(0, selected - 1);
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      selected = Math.min(searchResults.length - 1, selected + 1);
    }

    // scroll to selected
    const el = document.getElementById(searchResults[selected].id);
    el!.scrollIntoView({
      block: "nearest",
      inline: "nearest",
    });

    // if key is 0-4, set annotation
    if (event.key >= "0" && event.key <= "4") {
      searchResults[selected].annotation = parseInt(event.key);
      save();
    } else if (event.key === "§") {
      searchResults[selected].annotation = 0;
      save();
    } else if (event.key === "Backspace" || event.key === "-") {
      searchResults[selected].annotation = null;
      save();
    }

    // go to next/previous query
    if (event.key === "ArrowLeft") {
      selected = 0;
      window.location.href = `/annotate/${data.previousQuery?.qid}`;
    } else if (event.key === "ArrowRight") {
      selected = 0;
      window.location.href = `/annotate/${data.nextQuery?.qid}`;
    }

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
    <section class="flex flex-col space-y-3">
      {#each searchResults as res, i (res.id)}
        <div id={res.id}>
          <div class="relative">
            <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
            <div role="listitem" class="py-2 px-2 {selected == i ? 'bg-slate-300' : 'bg-slate-100'} border shadow-sm rounded-lg" on:click={() => selected = i} on:keypress={handleKeyPress}>
              <div class="w-full flex justify-between">
                <a href="{res.webpage.url}" target="_blank">{res.webpage.url}</a>
                {#if res.annotation != null}
                  {#if res.annotation == 4}
                    <div class="text-green-600">{res.annotation}</div>
                  {:else if res.annotation == 3}
                    <div class="text-green-500">{res.annotation}</div>
                  {:else if res.annotation == 2}
                    <div class="text-yellow-500">{res.annotation}</div>
                  {:else if res.annotation == 1}
                    <div class="text-red-500">{res.annotation}</div>
                  {:else if res.annotation == 0}
                    <div class="text-red-600">{res.annotation}</div>
                  {/if}
                {/if}
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

<svelte:window on:keydown={handleKeyPress}/>