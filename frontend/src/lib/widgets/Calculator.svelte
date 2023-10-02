<script lang="ts">
  import { browser } from '$app/environment';
  import type { Calculation } from '$lib/api';

  export let calc: Calculation;

  let input: string = calc.input;
  let result: string = calc.result;

  $: {
    if (browser)
      (async () => {
        const wasm = await import('wasm');
        result = wasm.fend_math(input);
      })();
  }
</script>

<div class="flex flex-col items-end rounded-xl border p-5">
  <div class="flex w-fit items-center space-x-0.5 text-xs text-neutral">
    <input
      type="text"
      class="m-0 w-64 rounded-sm border-none bg-transparent p-1 text-right text-xs"
      bind:value={input}
    /><span>=</span>
  </div>
  <div class="flex w-fit text-3xl font-bold">
    {result}
  </div>
</div>
