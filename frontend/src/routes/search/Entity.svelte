<script lang="ts">
  import type { Entity } from '$lib/api';

  export let entity: Entity;
</script>

<div class="flex w-full justify-center">
  <div class="flex w-full flex-col items-center">
    {#if entity.imageBase64}
      <div class="w-lg mb-5">
        <a href="https://en.wikipedia.org/wiki/{encodeURI(entity.title)}">
          <img
            alt="Image of {entity.title}"
            class="h-full w-full rounded-full"
            src="data:image/png;base64, {entity.imageBase64}"
          />
        </a>
      </div>
    {/if}
    <div class="mb-5 text-xl">
      <a class="hover:underline" href="https://en.wikipedia.org/wiki/{encodeURI(entity.title)}">
        {entity.title}
      </a>
    </div>
    <div class="text-sm">
      <span>{@html entity.smallAbstract}</span>{' '}
      <span class="italic">
        source:{' '}
        <a
          class="text-link visited:text-link-visited hover:underline"
          href="https://en.wikipedia.org/wiki/{encodeURI(entity.title)}"
        >
          wikipedia
        </a>
      </span>
    </div>
    {#if entity.info.length > 0}
      <div class="mb-2 mt-7 flex w-full flex-col px-4 text-sm">
        <div class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2">
          {#each entity.info as [key, value] (key)}
            <div class="text-neutral">{@html key}</div>
            <div>{@html value}</div>
          {/each}
        </div>
      </div>
    {/if}
    {#if entity.relatedEntities.length > 0}
      <div class="mt-5 flex w-full flex-col text-neutral">
        <div class="font-light">Related Searches</div>
        <div class="flex overflow-scroll">
          {#each entity.relatedEntities as related (related.title)}
            <div class="flex flex-col items-center p-4">
              {#if related.imageBase64}
                <div class="mb-3 h-20 w-20">
                  <a href="/search?q={encodeURIComponent(related.title)}">
                    <img
                      alt="Image of {related.title}"
                      class="h-full w-full rounded-full object-cover"
                      src="data:image/png;base64, {related.imageBase64}"
                    />
                  </a>
                </div>
              {/if}

              <div class="line-clamp-3 text-center">
                <a href="/search?q={encodeURI(related.title)}">
                  {related.title}
                </a>
              </div>
            </div>
          {/each}
        </div>
      </div>
    {/if}
  </div>
</div>
