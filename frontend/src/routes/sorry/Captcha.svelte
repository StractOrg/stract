<script lang="ts">
  import { page } from '$app/stores';

  export let resultDigestBase64: string;
</script>

<div class="flex w-full flex-col items-center pt-10">
  <div class="flex max-w-xl flex-col items-center space-y-10">
    <div class="flex flex-col space-y-2">
      <p class="text-xl">If you're human, we are really sorry that you have to do this.</p>
      <p class="text-sm">
        We have gotten an enormous increase in bots scraping our search results and have needed to
        implement a captcha to ensure we can continue serving search results to humans instead of
        bots. Sorry for the annoyance.
      </p>
    </div>
    <form class="flex flex-col space-y-2" method="get">
      <input type="text" class="hidden" name="digest" value={resultDigestBase64} />
      <input
        type="text"
        class="hidden"
        name="redirectTo"
        value={$page.url.searchParams.get('redirectTo') || '/'}
      />

      <slot />
    </form>
  </div>
</div>
