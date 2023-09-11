<script lang="ts">
  import IconGitHub from '~icons/simple-icons/github';
  import IconDiscord from '~icons/simple-icons/discord';
  import Bars2 from '~icons/heroicons/bars-2';

  import { twJoin } from 'tailwind-merge';
  import { page } from '$app/stores';
  import Link from './Link.svelte';
  import { searchQueryStore } from '$lib/stores';

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
    ['https://discord.gg/BmzKHffWJM', 'Join our Discord server', IconDiscord],
    ['https://github.com/StractOrg/stract', 'Read the source code at GitHub', IconGitHub],
  ] as const;
</script>

<div class="relative grid w-full grid-cols-[2fr_1fr_2fr] px-4 text-sm">
  <div class="flex space-x-4">
    {#each links as [url, name]}
      <a
        href={url}
        class={twJoin(
          'relative z-10 border-b p-2',
          $page.url.pathname.startsWith(url.split('?')[0])
            ? 'border-contrast-500'
            : 'border-transparent',
        )}
      >
        {name}
      </a>
    {/each}
  </div>

  <div class="flex items-center justify-center">
    {#if showLogo}
      <a href="/" class="w-20">
        <img alt="Stract logo" class="block dark:hidden" src="/images/biglogo-beta.svg" />
        <img
          alt="Stract logo dark version"
          aria-hidden="true"
          class="hidden dark:block"
          src="/images/biglogo-beta-alt.svg"
        />
      </a>
    {/if}
  </div>

  <nav class="hidden items-center justify-end space-x-1 sm:flex md:space-x-2 lg:space-x-4">
    {#each nav as [url, name]}
      <Link href={url}>
        {name}
      </Link>
    {/each}
    {#each social as [url, label, Icon]}
      <Link href={url} {label} round>
        <Icon />
      </Link>
    {/each}
  </nav>

  <nav class="group relative flex items-center justify-end text-lg sm:hidden">
    <button
      class="mx-1 aspect-square rounded-full bg-transparent px-3 text-gray-400 transition group-hover:text-brand-200"
    >
      <Bars2 />
    </button>
    <div
      class="pointer-events-none absolute bottom-0 right-0 z-50 translate-y-full flex-col pt-1 opacity-0 transition group-hover:pointer-events-auto group-hover:flex group-hover:opacity-100"
    >
      <div class="rounded-xl border bg-white p-2 shadow-xl">
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
              <Icon />
            </Link>
          {/each}
        </div>
      </div>
    </div>
  </nav>

  {#if showDivider}
    <div
      class="absolute inset-x-0 -bottom-0 h-px bg-gradient-to-r from-brand-400 via-brand-500 to-brand-400"
    />
  {/if}
</div>