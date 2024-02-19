<script lang="ts">
  import Article from '$lib/components/Article.svelte';
  import type { PageData } from './$types';
  import IconDiscord from '~icons/simple-icons/discord';
  import IconMatrix from '~icons/simple-icons/matrix';
  import Link from '$lib/components/Link.svelte';

  export let data: PageData;
</script>

<Article>
  {@html data.md}

  <div class="mt-10 flex justify-center">
    <div class="flex space-x-10">
      <Link href="https://discord.gg/BmzKHffWJM" label="Join our Discord server" round>
        <IconDiscord />
      </Link>
      <Link href="https://matrix.to/#/#stract:matrix.org" label="Join our Matrix server" round>
        <IconMatrix />
      </Link>
    </div>
  </div>
</Article>
