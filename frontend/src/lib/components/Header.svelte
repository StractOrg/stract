<script lang="ts">
  import IconGitHub from '~icons/simple-icons/github';
  import Bars2 from '~icons/heroicons/bars-2-20-solid';

  import { twJoin } from 'tailwind-merge';
  import { page } from '$app/stores';
  import Link from './Link.svelte';
  import { searchQueryStore } from '$lib/stores';
  import BiglogoBeta from '$lib/images/BiglogoBeta.svelte';

  $: links = [
    [`/search${typeof $searchQueryStore == 'string' ? $searchQueryStore : ''}`, 'Search'],
    ['/explore', 'Explore'],
  ] as const;

  $: showDivider = $page.data.globals?.header?.divider;
  $: showLogo = !$page.data.globals?.header?.hideLogo;

  const nav = [
    ['/settings', 'Settings'],
    ['/about', 'About'],
  ] as const;

  const social = [
    ['https://github.com/StractOrg/stract', 'Read the source code at GitHub', IconGitHub],
  ] as const;
</script>

<nav class="relative grid w-full grid-cols-[2fr_1fr_2fr] px-4 text-sm">
  <div class="flex space-x-4">
    {#each links as [url, name]}
      <a
        href={url}
        class={twJoin(
          'relative z-10 border-b p-2 text-neutral-focus',
          $page.url.pathname.startsWith(url.split('?')[0]) ? 'border-accent' : 'border-transparent',
        )}
      >
        {name}
      </a>
    {/each}
  </div>

  <div class="flex items-center justify-center">
    {#if showLogo}
      <a href="/" class="w-20" title="Go to Stract's frontpage">
        <BiglogoBeta />
      </a>
    {/if}
  </div>

  <div class="hidden items-center justify-end space-x-1 sm:flex md:space-x-2 lg:space-x-4">
    {#each nav as [url, name]}
      <Link href={url}>
        {name}
      </Link>
    {/each}
    {#each social as [url, label, Icon]}
      <Link href={url} {label} round>
        <Icon aria-label={label} />
      </Link>
    {/each}
  </div>

  <div class="group relative flex items-center justify-end text-lg sm:hidden">
    <button
      class="mx-1 aspect-square rounded-full bg-transparent px-3 text-neutral transition group-hover:text-neutral-focus"
    >
      <Bars2 />
    </button>
    <div
      class="pointer-events-none absolute bottom-0 right-0 z-50 translate-y-[calc(100%-1px)] flex-col pt-1 opacity-0 transition group-hover:pointer-events-auto group-hover:flex group-hover:opacity-100"
    >
      <div class="rounded-xl border bg-base-100 p-2 shadow-xl">
        <div class="flex flex-col items-start space-y-1 pb-2">
          {#each nav as [url, name]}
            <Link href={url}>
              {name}
            </Link>
          {/each}
        </div>
        <div class="flex justify-around border-t pt-2">
          {#each social as [url, label, Icon]}
            <Link href={url} {label} round>
              <Icon aria-label={label} />
            </Link>
          {/each}
        </div>
      </div>
    </div>
  </div>

  {#if showDivider}
    <div
      class="absolute inset-x-0 -bottom-0 h-px bg-gradient-to-r from-primary via-primary-focus to-primary"
    />
  {/if}
</nav>
