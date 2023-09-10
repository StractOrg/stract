<script lang="ts">
  import Button from '$lib/components/Button.svelte';
</script>

<main class="mx-auto flex w-full max-w-xl flex-col space-y-10 py-10">
  <section class="flex flex-col space-y-2">
    <h1 class="text-3xl font-semibold">Buttons</h1>
    <div class="rounded-xl border p-4 shadow-xl">
      <Button>click me</Button>
      <Button kind="amber">click me</Button>
      <Button kind="red">click me</Button>
      <hr class="my-2" />
      <Button pale>click me</Button>
      <Button pale kind="amber">click me</Button>
      <Button pale kind="red">click me</Button>
    </div>
  </section>
</main>
