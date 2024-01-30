<script lang="ts">
  import { allowStatsStore } from '$lib/stores';
</script>

<div class="flex flex-col space-y-3 [&_a]:font-medium [&_a]:underline">
  <h1 class="text-2xl font-medium">Privacy</h1>
  <div class="text-sm">
    By default, some data about your searches are stored on our servers. We primarily store the text
    you used for the search, and which results (if any) you clicked on. This data immensely helps us
    improve everyones search experience by training our ranking models.
  </div>

  <div class="text-sm">
    It is important to note, that <b class="font-extrabold">no</b>{' '}
    personal information is stored about your search. We don't store your IP, browser fingerprint or
    even a precise timestamp. In fact, you can see exactly what gets stored{' '}
    <a href="https://github.com/StractOrg/Stract/blob/main/crates/core/src/improvement.rs#L37-#L51">here</a
    >.
  </div>

  <div class="text-sm">
    We hope you feel comfortable sharing this data with us as this makes the results much better for
    everyone, but we also respect if you don't. You can opt-out at any point by toggling the
    checkbox below and we wont store anything at all about your searches.
  </div>

  <div class="noscript:hidden flex space-x-1 pl-1 text-sm">
    <input type="checkbox" id="allow-stats" bind:checked={$allowStatsStore} />
    <label for="allow-stats">Allow usage statistics</label>
  </div>
</div>
