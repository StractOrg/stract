<script lang="ts">
  import XMark from '~icons/heroicons/x-mark';
  import PlusCircleOutline from '~icons/heroicons/plus-circle';
  import ChevronDown from '~icons/heroicons/chevron-down';
  import { api } from '$lib/api';
  import Button from '$lib/components/Button.svelte';
  import Site from '$lib/components/Site.svelte';
  import Select from '$lib/components/Select.svelte';
  import { flip } from 'svelte/animate';
  import { fade, slide } from 'svelte/transition';
  import { twJoin } from 'tailwind-merge';
  import { match } from 'ts-pattern';
  import Callout from '$lib/components/Callout.svelte';
  import type { PageData } from './$types';
  import { LIMIT_OPTIONS } from './conf';
  import { page } from '$app/stores';
  import { goto } from '$app/navigation';
  import { browser } from '$app/environment';
  import { InvalidHostError, UnknownHostError } from '.';

  export let data: PageData;

  let inputWebsite = '';

  let limit = data.limit ?? LIMIT_OPTIONS[0];
  let chosenHosts = data.chosenHosts ?? [];
  let similarHosts = data.similarHosts ?? [];
  let errorMessage = data.errorMessage;

  $: chosenHostString = chosenHosts.join(',');

  $: {
    api.webgraphHostSimilar({ hosts: chosenHosts, topN: limit }).data.then((res) => {
      similarHosts = res;
    });
  }

  $: {
    limit;
    updateBrowserState();
  }

  const updateBrowserState = () => {
    if (!browser) {
      return;
    }
    $page.url.searchParams.set('chosenHosts', chosenHosts.join(','));
    $page.url.searchParams.set('limit', limit.toString());
    goto($page.url, { replaceState: true });
  };

  $: {
    if (browser) {
      chosenHosts =
        $page.url.searchParams
          .get('chosenHosts')
          ?.split(',')
          .filter((host) => host.length > 0) ?? [];
    }
  }

  const removeWebsite = async (host: string) => {
    if (chosenHosts.includes(host)) {
      chosenHosts = chosenHosts.filter((s) => s != host);
      updateBrowserState();
    }
  };

  const addWebsite = async (host: string, clear = false) => {
    errorMessage = undefined;
    host = host.trim();
    if (!host) return;

    try {
      const result = await api.webgraphHostKnows({ host }).data;
      match(result)
        .with({ _type: 'unknown' }, () => {
          errorMessage = UnknownHostError;
        })
        .with({ _type: 'known' }, async ({ host }) => {
          if (clear) inputWebsite = '';
          if (!chosenHosts.includes(host)) chosenHosts = [...chosenHosts, host];
          updateBrowserState();
        })
        .exhaustive();
    } catch (_) {
      errorMessage = InvalidHostError;
    }
  };

  const exportAsOptic = async () => {
    const { data } = api.exploreExport({
      chosenHosts: chosenHosts,
      similarHosts: similarHosts.map((host) => host.host),
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
        <input class="hidden" type="text" name="chosenHosts" bind:value={chosenHostString} />
        <Button>Add</Button>
      </form>
      <noscript>
        <div class="mb-2">
          <form>
            <Button pale kind="info">Clear All Sites</Button>
          </form>
        </div>
      </noscript>
      {#if errorMessage}
        <div class="my-2" transition:slide>
          <Callout kind="warning" title="Unable to add page">
            <button slot="top-right" on:click={() => (errorMessage = undefined)} title="Close">
              <XMark aria-label="X-mark" />
            </button>

            {errorMessage}
          </Callout>
        </div>
      {/if}
      <div class="flex flex-wrap justify-center gap-x-5 gap-y-3" id="sites-list">
        {#each chosenHosts as site (`${site}`)}
          <div transition:slide={{ duration: 100 }} animate:flip={{ duration: 200 }}>
            <Site href="http://{site}" on:delete={() => removeWebsite(site)}>
              {site}
            </Site>
          </div>
        {/each}
      </div>
    </div>

    {#if chosenHosts.length > 0 && similarHosts.length > 0}
      <div class="flex flex-col space-y-4">
        <div class="grid grid-cols-[auto_auto_1fr_auto] items-center gap-5">
          <h2 class="text-2xl font-bold">Similar sites</h2>
          <div class="flex space-x-1">
            <Select
              id="limit"
              name="limit"
              form="site-input-container"
              class="cursor-pointer rounded border-none dark:bg-transparent"
              bind:value={limit}
              options={LIMIT_OPTIONS.map((value) => ({ value, label: value.toString() }))}
              title="Limit number of similar sites shown"
            />
          </div>
          <div />
          <Button _class="noscript:hidden" on:click={exportAsOptic}>Export as optic</Button>
        </div>
        <table class="w-full border-separate border-spacing-y-1">
          <thead>
            <tr class="text-left">
              <th class="pb-2" aria-label="Add to search"></th>
              <th class="pb-2">Similarity</th>
              <th class="pb-2">Site</th>
            </tr>
          </thead>
          <tbody>
            {#each similarHosts as host (`${host.host}`)}
              <tr
                class="items-center"
                transition:fade={{ duration: 200 }}
                animate:flip={{ duration: 200 }}
              >
                <td class="pr-2">
                  <button
                    class={twJoin('noscript:hidden group')}
                    on:click={() =>
                      chosenHosts.includes(host.host)
                        ? removeWebsite(host.host)
                        : addWebsite(host.host)}
                    title={chosenHosts.includes(host.host) ? 'Remove from search' : 'Add to search'}
                  >
                    <PlusCircleOutline
                      class={twJoin(
                        'text-xl text-success group-hover:scale-105 group-active:scale-95',
                      )}
                      aria-label="Plus-mark"
                    />
                  </button>
                </td>
                <td class="px-2">{host.score.toFixed(2)}</td>
                <td>
                  <a href="http://{host.host}" target="_blank" class="underline">{host.host}</a>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
        <div class="noscript:hidden flex w-full justify-center">
          <button
            class="h-6 w-6 cursor-pointer rounded-full text-accent"
            title="Show more similar sites"
            on:click={() => {
              if (limit == LIMIT_OPTIONS[LIMIT_OPTIONS.length - 1]) {
                return;
              }
              limit = LIMIT_OPTIONS[LIMIT_OPTIONS.indexOf(limit) + 1];
            }}
          >
            <ChevronDown aria-label="Chevron-down" />
          </button>
        </div>
      </div>
    {/if}
  </div>
</div>
