<script lang="ts">
  const footer = [
    ['/privacy-and-happy-lawyers', 'Terms & Privacy'],
    ['/beta/api/docs', 'API'],
  ] as const;
</script>

<div class="flex w-full items-center justify-center pb-10 pt-5 text-sm">
  <div class="flex space-x-10">
    {#each footer as [url, name]}
      <a href={url} class="hover:underline">{name}</a>
    {/each}
  </div>
</div>
