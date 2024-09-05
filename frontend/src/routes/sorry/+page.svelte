<script lang="ts">
  import Speaker from '~icons/heroicons/speaker-wave-20-solid';
  import Button from '$lib/components/Button.svelte';
  import type { PageData } from './$types';
  import Visual from './Visual.svelte';
  import Captcha from './Captcha.svelte';
  import { page } from '$app/stores';

  export let data: PageData;
</script>

<Captcha resultDigestBase64={data.resultDigestBase64}>
  <Visual imgBase64={data.imgBase64} animal={data.animal} count={data.count} />

  <div class="flex w-full justify-between">
    <a href={`sorry/audio${$page.url.search}`} title="Get an audio challenge"
      ><Speaker class="h-8 w-8" /></a
    >
    <Button title="Verify challenge" type="submit" kind="accent">VERIFY</Button>
  </div>
</Captcha>
