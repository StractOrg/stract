<script lang="ts">
  import MinusCircle from '~icons/heroicons/minus-circle';
  import EyeSlash from '~icons/heroicons/eye-slash';
  import Eye from '~icons/heroicons/eye';
  import Button from '$lib/components/Button.svelte';
  import { opticsShowStore, opticsStore } from '$lib/stores';
  import { DEFAULT_OPTICS, opticKey, type OpticOption } from '$lib/optics';
  import { derived } from 'svelte/store';
  import Callout from '$lib/components/Callout.svelte';

  let name = '';
  let url = '';
  let description = '';

  let error: TypeError | undefined;

  const addOptic = async () => {
    error = void 0;
    try {
      const res = await fetch('optics/validate', {
        method: 'post',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          opticUrl: url,
        }),
      });

      if (res.status != 200) {
        const desc = await res.text();
        throw TypeError(desc);
      }

      opticsShowStore.update(($opticsShow) => ({
        ...$opticsShow,
        [opticKey({ name, url, description })]: true,
      }));
      opticsStore.update(($optics) => [...$optics, { name, url, description, shown: true }]);

      name = '';
      url = '';
      description = '';
    } catch (e) {
      if (e instanceof TypeError) {
        error = e;
      }
      console.error('Failed to add optic', e);
    }
  };

  const removeOptic = (optic: OpticOption) => () => {
    opticsStore.update(($optics) => $optics.filter((o) => o != optic));
    opticsShowStore.update(($opticsShow) => {
      const { [opticKey(optic)]: _, ...rest } = $opticsShow;
      return rest;
    });
  };

  for (const optic of DEFAULT_OPTICS) {
    if ($opticsShowStore[opticKey(optic)] !== undefined) continue;

    opticsShowStore.update(($opticsShow) => ({
      ...$opticsShow,
      [opticKey(optic)]: optic.shown,
    }));
  }

  const optics = derived(opticsStore, ($optics) => [
    ...$optics.map((optic) => ({
      optic,
      removable: true,
    })),
    ...DEFAULT_OPTICS.map((optic) => ({
      optic,
      removable: false,
    })),
  ]);
</script>

<div class="space-y-16">
  <div class="space-y-3">
    <h1 class="text-2xl font-medium">Manage Optics</h1>
    <div class="text-sm">
      Optics lets you control almost everything about which search results that gets returned to
      you. You can discard results from specific sites, boost results from other sites and much
      more.
    </div>
    <div class="text-sm">
      See our
      <a
        href="https://github.com/StractOrg/sample-optics/blob/main/quickstart.optic"
        class="inline-flex font-medium underline"
      >
        quickstart
      </a>
      for how to get started. Once you have developed your optic, you can add it here to be used during
      your search.
    </div>
    <div class="text-sm">
      Simply host the optic on a url that returns a plain-text HTTP response with the optic. We use
      raw.githubusercontent.com, but you are free to host them elsewhere.
    </div>
  </div>

  <div class="flex flex-col space-y-4">
    <form
      class="noscript:hidden grid w-full grid-cols-[1fr_1fr_5rem] gap-x-5 gap-y-2 px-8 [&>input]:rounded [&>input]:border-none [&>input]:bg-transparent"
      on:submit|preventDefault={addOptic}
    >
      <input
        type="text"
        required
        placeholder="Name"
        name="Name"
        autocomplete="off"
        bind:value={name}
      />
      <input type="url" required placeholder="Url" name="Url" bind:value={url} />
      <Button title="Remove optic">Add</Button>
      <input
        class="col-span-2"
        type="text"
        name="Description"
        placeholder="Description"
        bind:value={description}
      />
    </form>
    {#if error}
      <Callout kind="error" title="Validating optic failed">
        <p>
          Failed while trying to <code class="text-sm">fetch</code> the optic url. Check your browser
          console for details.
        </p>
      </Callout>
    {/if}
    <div class="mt-5">
      <div class="grid w-full grid-cols-[auto_auto_1fr_2fr_auto] gap-5" id="optics-list">
        <span />
        <span />
        <div class="flex-1 font-medium">Name</div>
        <div class="flex-1 font-medium">Description</div>
        <div class="flex-1 font-medium">Link</div>
        {#each $optics as { optic, removable }}
          <button
            class="group flex w-6 items-start !bg-transparent"
            on:click={removeOptic(optic)}
            disabled={!removable}
          >
            <MinusCircle
              class="text-neutral transition group-enabled:text-error group-enabled:group-hover:text-error"
            />
          </button>
          <label class="flex w-6 items-start hover:cursor-pointer">
            <input
              type="checkbox"
              bind:checked={$opticsShowStore[opticKey(optic)]}
              class="peer hidden"
            />
            <EyeSlash class="inline-flex text-neutral transition peer-checked:hidden" />
            <Eye class="hidden text-neutral transition peer-checked:inline-flex" />
          </label>
          <div class="text-sm">{optic.name}</div>
          <div class="text-sm">
            {optic.description}
          </div>
          <div class="text-sm font-medium underline">
            <a href={optic.url}>Source</a>
          </div>
        {/each}
      </div>
    </div>
  </div>
</div>
