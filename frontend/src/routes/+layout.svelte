<script lang="ts">
  import '../app.css';
  import Header from '$lib/components/Header.svelte';
  import Footer from '$lib/components/Footer.svelte';
  import { page } from '$app/stores';
</script>

<svelte:head>
  <title>{$page.data.globals?.title || 'Stract'}</title>
  <meta property="og:title" content="Stract" />
  <meta property="og:image" content="https://stract.com/images/biglogo-with-text.png" />
  <meta name="viewport" content="width=device-width" />
</svelte:head>

<div class="grid min-h-screen grid-rows-[auto_1fr_auto] font-light">
  <Header />

  <div id="main">
    <slot />
  </div>

  <Footer />
</div>

<noscript>
  <style lang="postcss">
    .noscript\:hidden {
      @apply hidden;
    }
  </style>
</noscript>
