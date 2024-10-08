<script lang="ts">
  import Link from '$lib/components/Link.svelte';

  export let query = '';

  const links = [
    { url: '/settings', title: 'Preferences' },
    { url: '/settings/optics', title: 'Manage Optics' },
    { url: '/settings/sites', title: 'Site Rankings' },
    { url: '/settings/privacy', title: 'Privacy' },
  ];
</script>

<div class="flex w-full justify-center pt-10">
  <div class="relative right-0 flex flex-col space-y-3 md:right-5 lg:right-20">
    {#each links as l}
      <Link href="{l.url}{query ? '?' + query : ''}" exact>
        {l.title}
      </Link>
    {/each}
  </div>
  <slot />
</div>
