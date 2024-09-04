<script lang="ts">
  import Photo from '~icons/heroicons/photo-20-solid';
  import Button from '$lib/components/Button.svelte';
  import type { PageData } from './$types';
  import Captcha from '../Captcha.svelte';
  import Audio from '../Audio.svelte';
  import { page } from '$app/stores';

  export let data: PageData;

  $: console.log(data.audioBase64);
</script>

<Captcha resultDigestBase64={data.resultDigestBase64}>
  <Audio audioBase64={data.audioBase64} />
  <input
    type="number"
    class="appearance-none rounded-lg border bg-transparent"
    placeholder="12345"
    name="challenge"
    title="Type the numbers you hear in the audio file"
  />

  <div class="flex w-full justify-between">
    <a href={`/sorry${$page.url.search}`} title="Get a visual challenge"
      ><Photo class="h-8 w-8" /></a
    >

    <Button title="Verify challenge" type="submit">VERIFY</Button>
  </div>
</Captcha>

<style>
  input::-webkit-outer-spin-button,
  input::-webkit-inner-spin-button {
    appearance: none;
  }

  /* Firefox */
  input[type='number'] {
    appearance: textfield;
  }
</style>
