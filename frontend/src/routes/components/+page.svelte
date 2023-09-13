<script lang="ts">
  import IconGitHub from '~icons/simple-icons/github';
  import IconDiscord from '~icons/simple-icons/discord';
  import HandThumbUp from '~icons/heroicons/hand-thumb-up-20-solid';
  import Button from '$lib/components/Button.svelte';
  import Callout from '$lib/components/Callout.svelte';
  import Link from '$lib/components/Link.svelte';
  import { THEMES } from '$lib/themes';
  import { themeStore } from '$lib/stores';
</script>

<main class="relative mx-auto flex w-full max-w-2xl flex-col space-y-10 py-10">
  <div
    class="fixed translate-x-[calc(-100%-5rem)] space-y-2 rounded border bg-base-200 px-2 py-2 shadow"
  >
    <h1 class="text-2xl font-semibold">Themes</h1>
    <div class="flex flex-col space-y-1">
      {#each THEMES as theme}
        <button
          class="flex space-x-2 p-1 {theme.class} items-center rounded border bg-base-100 text-base-content"
          on:mouseenter={() => ($themeStore = theme.class)}
        >
          <span class="inline-flex flex-1 justify-center px-2 text-center font-bold">
            <span>
              {theme.name}
            </span>
          </span>
          <div class="flex overflow-hidden rounded border [&>*]:aspect-square [&>*]:w-10">
            <div class="bg-primary" />
            <div class="bg-secondary" />
            <div class="bg-accent" />
          </div>
        </button>
      {/each}
    </div>
  </div>

  <section class="flex flex-col space-y-2">
    <h1 class="text-3xl font-semibold">Buttons</h1>
    <div class="flex flex-wrap items-center gap-x-2 rounded-xl border p-4 shadow-xl">
      <Button>click me</Button>
      <Button padding={false}><HandThumbUp /></Button>
      <Button kind="seconday">click me</Button>
      <Button kind="seconday" padding={false}><HandThumbUp /></Button>
      <Button kind="accent">click me</Button>
      <Button kind="accent" padding={false}><HandThumbUp /></Button>
      <Button kind="neutral">click me</Button>
      <Button kind="neutral" padding={false}><HandThumbUp /></Button>
      <hr class="my-2 w-full" />
      <Button pale>click me</Button>
      <Button pale padding={false}><HandThumbUp /></Button>
      <Button pale kind="seconday">click me</Button>
      <Button pale kind="seconday" padding={false}><HandThumbUp /></Button>
      <Button pale kind="accent">click me</Button>
      <Button pale kind="accent" padding={false}><HandThumbUp /></Button>
      <Button pale kind="neutral">click me</Button>
      <Button pale kind="neutral" padding={false}><HandThumbUp /></Button>
      <hr class="my-2 w-full" />
      <Button kind="info">click me</Button>
      <Button kind="info" padding={false}><HandThumbUp /></Button>
      <Button kind="success">click me</Button>
      <Button kind="success" padding={false}><HandThumbUp /></Button>
      <Button kind="warning">click me</Button>
      <Button kind="warning" padding={false}><HandThumbUp /></Button>
      <Button kind="error">click me</Button>
      <Button kind="error" padding={false}><HandThumbUp /></Button>
      <hr class="my-2 w-full" />
      <Button pale kind="info">click me</Button>
      <Button pale kind="info" padding={false}><HandThumbUp /></Button>
      <Button pale kind="success">click me</Button>
      <Button pale kind="success" padding={false}><HandThumbUp /></Button>
      <Button pale kind="warning">click me</Button>
      <Button pale kind="warning" padding={false}><HandThumbUp /></Button>
      <Button pale kind="error">click me</Button>
      <Button pale kind="error" padding={false}><HandThumbUp /></Button>
    </div>
  </section>
  <section class="flex flex-col space-y-2">
    <h1 class="text-3xl font-semibold">Links</h1>
    <!-- svelte-ignore a11y-invalid-attribute -->
    <div class="rounded-xl border p-4 shadow-xl">
      <div class="flex items-center space-x-2">
        <Link href="#">This is a</Link>
        <Link href="#">Link to somewhere</Link>
        <Link href="#" round><IconDiscord /></Link>
        <Link href="#" round><IconGitHub /></Link>
        <a href="#" class="inline-flex pl-4 text-link">I haven't been here</a>
      </div>
      <hr class="my-2" />
      <div class="flex items-center space-x-2">
        <Link href="#" active>This is a</Link>
        <Link href="#" active>Link to somewhere</Link>
        <Link href="#" active round><IconDiscord /></Link>
        <Link href="#" active round><IconGitHub /></Link>
        <a href="#" class="inline-flex pl-4 text-link-visited">I have been here</a>
      </div>
    </div>
  </section>
  <section class="flex flex-col space-y-2">
    <h1 class="text-3xl font-semibold">Callouts</h1>
    <div class="space-y-4 rounded-xl border p-4 shadow-xl">
      <Callout kind="info">This is a info callout</Callout>
      <Callout kind="success">This is a success callout</Callout>
      <Callout kind="warning">This is a warning callout</Callout>
      <Callout kind="error">This is a error callout</Callout>
      <Callout kind="neutral">This is a neutral callout</Callout>
    </div>
  </section>
  <section class="flex flex-col space-y-2">
    <h1 class="text-3xl font-semibold">Colors</h1>
    <div class="flex flex-wrap gap-4 rounded-xl border p-4 shadow-xl">
      <div class="color-sample">
        <h2>primary</h2>
        <div>
          <div class="bg-primary" />
          <div class="bg-primary-focus" />
          <div class="bg-primary-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>secondary</h2>
        <div>
          <div class="bg-secondary" />
          <div class="bg-secondary-focus" />
          <div class="bg-secondary-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>accent</h2>
        <div>
          <div class="bg-accent" />
          <div class="bg-accent-focus" />
          <div class="bg-accent-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>neutral</h2>
        <div>
          <div class="bg-neutral" />
          <div class="bg-neutral-focus" />
          <div class="bg-neutral-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>base</h2>
        <div>
          <div class="bg-base-100" />
          <div class="bg-base-200" />
          <div class="bg-base-300" />
          <div class="bg-base-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>info</h2>
        <div>
          <div class="bg-info" />
          <div class="bg-info-focus" />
          <div class="bg-info-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>success</h2>
        <div>
          <div class="bg-success" />
          <div class="bg-success-focus" />
          <div class="bg-success-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>warning</h2>
        <div>
          <div class="bg-warning" />
          <div class="bg-warning-focus" />
          <div class="bg-warning-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>error</h2>
        <div>
          <div class="bg-error" />
          <div class="bg-error-focus" />
          <div class="bg-error-content" />
        </div>
      </div>
      <div class="color-sample">
        <h2>link</h2>
        <div>
          <div class="bg-link" />
          <div class="bg-link-visited" />
        </div>
      </div>
    </div>
  </section>
</main>

<style lang="postcss">
  .color-sample h2 {
    @apply mb-1 text-lg font-semibold capitalize;
  }
  .color-sample > div {
    @apply inline-flex overflow-hidden rounded border border-base-300 [&>*]:aspect-square [&>*]:w-8;
  }
</style>
