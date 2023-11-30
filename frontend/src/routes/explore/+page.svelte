<script lang="ts">
  import XMark from '~icons/heroicons/x-mark';
  import PlusCircleOutline from '~icons/heroicons/plus-circle';
  import ChevronDown from '~icons/heroicons/chevron-down';
  import { api, type ScoredSite } from '$lib/api';
  import Button from '$lib/components/Button.svelte';
  import Site from '$lib/components/Site.svelte';
  import Select from '$lib/components/Select.svelte';
  import { flip } from 'svelte/animate';
  import { fade, slide } from 'svelte/transition';
  import { twJoin } from 'tailwind-merge';
  import { match } from 'ts-pattern';
  import Callout from '$lib/components/Callout.svelte';

  const LIMIT_OPTIONS = [10, 25, 50, 125, 250, 500, 1000];

  let inputWebsite = '';
  let limit = LIMIT_OPTIONS[0];
  let chosenSites: string[] = [];
  let similarSites: ScoredSite[] = [];

  let errorMessage = false;

  $: {
    api
      .webgraphHostSimilar({ sites: chosenSites, topN: limit })
      .data.then((res) => (similarSites = res));
  }

  const removeWebsite = async (site: string) => {
    if (chosenSites.includes(site)) {
      chosenSites = chosenSites.filter((s) => s != site);
    }
  };
  const addWebsite = async (site: string, clear = false) => {
    errorMessage = false;
    site = site.trim();
    if (!site) return;

    const result = await api.webgraphHostKnows({ site }).data;
    match(result)
      .with({ type: 'unknown' }, () => {
        errorMessage = true;
      })
      .with({ type: 'known' }, async ({ site }) => {
        if (clear) inputWebsite = '';
        if (!chosenSites.includes(site)) chosenSites = [...chosenSites, site];
      })
      .exhaustive();
  };

  const exportAsOptic = async () => {
    const { data } = api.exploreExport({
      chosenSites,
      similarSites: similarSites.map((site) => site.site),
    });
    const optic = await data;
    const { default: fileSaver } = await import('file-saver');
    fileSaver.saveAs(new Blob([optic]), 'exported.optic');
  };
</script>

<div class="mt-10 flex justify-center px-5">
  <div class="noscirpt:hidden flex max-w-3xl grow flex-col">
    <div class="mb-4 flex flex-col items-center">
      <div class="mb-5 flex flex-col items-center space-y-1">
        <h1 class="text-2xl font-bold">Explore the web</h1>
        <p class="text-center">
          Find sites similar to your favorites and discover hidden gems you never knew existed.
        </p>
      </div>
      <form
        class={twJoin(
          'mb-2 flex w-full max-w-lg rounded-full border border-base-400 bg-base-100 p-[1px] pl-3 transition focus-within:shadow',
        )}
        id="site-input-container"
        on:submit|preventDefault={() => addWebsite(inputWebsite, true)}
      >
        <!-- svelte-ignore a11y-autofocus -->
        <input
          class="grow border-none bg-transparent outline-none placeholder:opacity-50 focus:ring-0"
          type="text"
          id="site-input"
          name="site"
          autofocus
          placeholder="www.example.com"
          bind:value={inputWebsite}
        />
        <Button>Add</Button>
      </form>
      {#if errorMessage}
        <div class="my-2" transition:slide>
          <Callout kind="warning" title="Unable to add page">
            <button slot="top-right" on:click={() => (errorMessage = false)}>
              <XMark />
            </button>

            Unfortunately, we don't know about that site yet.
          </Callout>
        </div>
      {/if}
      <div class="flex flex-wrap justify-center gap-x-5 gap-y-3" id="sites-list">
        {#each chosenSites as site (site)}
          <div transition:slide={{ duration: 100 }} animate:flip={{ duration: 200 }}>
            <Site
              href="http://{site}"
              on:delete={() => (chosenSites = chosenSites.filter((s) => s != site))}
            >
              {site}
            </Site>
          </div>
        {/each}
      </div>
    </div>

    {#if chosenSites.length > 0 && similarSites.length > 0}
      <div class="flex flex-col space-y-4">
        <div class="grid grid-cols-[auto_auto_1fr_auto] items-center gap-5">
          <h2 class="text-2xl font-bold">Similar sites</h2>
          <div class="flex space-x-1">
            <Select
              id="limit"
              class="cursor-pointer rounded border-none dark:bg-transparent"
              bind:value={limit}
              options={LIMIT_OPTIONS.map((value) => ({ value, label: value.toString() }))}
            />
          </div>
          <div />
          <Button on:click={exportAsOptic}>Export as optic</Button>
        </div>
        <div class="grid items-center gap-y-2">
          {#each similarSites as site (site.site)}
            <div
              class="col-span-full grid grid-cols-[auto_auto_minmax(auto,66%)] items-center gap-x-3"
              transition:fade={{ duration: 200 }}
              animate:flip={{ duration: 200 }}
            >
              <div>
                <button
                  class={twJoin('group')}
                  on:click={() =>
                    chosenSites.includes(site.site)
                      ? removeWebsite(site.site)
                      : addWebsite(site.site)}
                >
                  <PlusCircleOutline
                    class={twJoin(
                      'text-xl transition group-hover:scale-105 group-active:scale-95',
                      chosenSites.includes(site.site) ? 'rotate-45 text-error' : 'text-success',
                    )}
                  />
                </button>
              </div>
              <span>{site.score.toFixed(2)}</span>
              <div class="flex">
                <a href="http://{site.site}" target="_blank" class="underline">{site.site}</a>
              </div>
            </div>
          {/each}
        </div>
        <div class="flex w-full justify-center">
          <button
            class="h-6 w-6 cursor-pointer rounded-full text-accent"
            aria-label="Show more similar sites"
            on:click={() => {
              if (limit == LIMIT_OPTIONS[LIMIT_OPTIONS.length - 1]) {
                return;
              }
              limit = LIMIT_OPTIONS[LIMIT_OPTIONS.indexOf(limit) + 1];
            }}
          >
            <ChevronDown />
          </button>
        </div>
      </div>
    {/if}
  </div>
</div>
