<script lang="ts">
  import '../app.css';
  import Header from '$lib/components/Header.svelte';
  import Footer from '$lib/components/Footer.svelte';
  import { page } from '$app/stores';
</script>

<svelte:head>
  <title>{$page.data.globals?.title || 'Stract'}</title>
</svelte:head>

<div
  class="grid min-h-screen grid-rows-[auto_1fr_auto] font-light dark:bg-stone-900 dark:text-white"
>
  <Header />

  <slot />

  <Footer />
</div>

<noscript>
  <style lang="postcss">
    .noscript\:hidden {
      @apply hidden;
    }
  </style>
</noscript>
