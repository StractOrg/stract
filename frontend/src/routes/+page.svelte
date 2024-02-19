<script lang="ts">
  import OpticSelector from '$lib/components/OpticSelector.svelte';
  import Searchbar from '$lib/components/Searchbar.svelte';
  import BiglogoWithText from '$lib/images/BiglogoWithText.svelte';
</script>

<div class="mt-[25vh] flex w-full flex-1 flex-col items-center justify-start">
  <div class="mb-2 flex w-64">
    <BiglogoWithText />
  </div>
  <div class="w-full px-3 md:mx-0 md:max-w-xl">
    <Searchbar autofocus={true} />
  </div>
  <div class="mt-3 flex space-x-2 px-5 text-sm text-neutral md:text-sm">
    <div>
      Customise your search with an{' '}
      <a class="font-medium underline" href="/settings/optics">optic</a>:
    </div>
    <OpticSelector searchOnChange={false} />
  </div>
</div>
