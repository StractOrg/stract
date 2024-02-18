<script lang="ts">
  import '../app.css';
  import Header from '$lib/components/Header.svelte';
  import Footer from '$lib/components/Footer.svelte';
  import { i18n } from '$lib/i18n';
  import { page } from '$app/stores';
  import { ParaglideJS } from '@inlang/paraglide-js-adapter-sveltekit';
</script>

<svelte:head>
  <title>{$page.data.globals?.title || 'Stract'}</title>
  <meta property="og:title" content="Stract" />
  <meta property="og:image" content="https://stract.com/images/biglogo-with-text.png" />
  <meta name="viewport" content="width=device-width" />
</svelte:head>

<ParaglideJS {i18n}>
  <div class="grid min-h-screen grid-rows-[auto_1fr_auto] font-light">
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
</ParaglideJS>
