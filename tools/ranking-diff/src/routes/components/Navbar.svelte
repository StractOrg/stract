<script lang="ts">
  import Settings from './Settings.svelte';

  type Page = 'experiments' | 'queries';
  export let page: Page;
</script>

<nav class="flex justify-between gap-x-2">
  <div class="flex h-fit gap-x-2">
    <a
      href="/"
      class="border-content rounded-full border-slate-500 px-2 py-1 {page === 'experiments'
        ? 'border'
        : ''}"
    >
      Experiments
    </a>
    <a
      href="/queries"
      class="border-content rounded-full border-slate-500 px-2 py-1 {page === 'queries'
        ? 'border'
        : ''}"
    >
      Queries
    </a>
  </div>
  <div>
    <Settings />
  </div>
</nav>
