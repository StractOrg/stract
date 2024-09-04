<script lang="ts">
  import Patch from './Patch.svelte';
  import type { Animal } from '$lib/captcha/lib';

  export let imgBase64: string;
  export let animal: Animal;
  export let count: number;
</script>

<div>
  <p class="text-xl">Select <b>{count}</b> images with a <b>{animal}</b></p>
  <div class="relative w-fit">
    <img src={imgBase64} alt="captcha of animals" />
    <div class="absolute bottom-0 left-0 top-0 h-full w-full">
      {#key imgBase64}
        <Patch id={0} />
        <Patch id={1} />
        <Patch id={2} />
        <Patch id={3} />
        <Patch id={4} />
        <Patch id={5} />
        <Patch id={6} />
        <Patch id={7} />
        <Patch id={8} />
      {/key}
    </div>
  </div>
</div>
