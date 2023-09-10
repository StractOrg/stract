<script lang="ts">
  import OpticSelector from '$lib/components/OpticSelector.svelte';
  import Searchbar from '$lib/components/Searchbar.svelte';
</script>

<div class="mt-[25vh] flex w-full flex-1 flex-col items-center justify-start">
  <div class="mb-6 flex w-64">
    <img alt="stract logo" src="/images/biglogo-with-text.svg" class="h-full w-full" />
  </div>
  <div class="w-full md:mx-0 md:max-w-2xl">
    <Searchbar />
  </div>
  <div class="mt-3 flex space-x-2 text-gray-600 dark:text-gray-500">
    <div>
      Customise your search with an{' '}
      <a class="font-medium underline" href="/settings/optics">optic</a>:
    </div>
    <OpticSelector searchOnChange={false} />
  </div>
</div>
