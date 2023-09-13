<script lang="ts">
  import { page } from '$app/stores';
</script>

<main class="grid h-full place-items-center">
  <div class="flex flex-col items-center justify-center text-center">
    <h1 class="text-4xl">{$page.error?.message}</h1>
    <p class="text-xl"><span class="select-none text-transparent">...</span>An error occured...</p>
    <br />
    <a
      class="rounded bg-base-200 px-2 py-1 text-neutral"
      href={$page.url.toString()}
      on:click|preventDefault={() => location.reload()}>Try again</a
    >
  </div>
</main>
