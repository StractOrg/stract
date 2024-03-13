<script lang="ts">
  import { getApiBase, type DisplayedEntity } from '$lib/api';
  import EntitySnippet from '$lib/components/EntitySnippet.svelte';
  import ResultLink from './ResultLink.svelte';

  export let entity: DisplayedEntity;
</script>

<div class="flex w-full justify-center">
  <div class="flex w-full flex-col items-center">
    {#if entity.imageId}
      <div class="w-lg mb-5">
        <ResultLink href="https://en.wikipedia.org/wiki/{encodeURI(entity.title)}">
          <div class="h-40">
            <img
              alt="Image of {entity.title}"
              class="h-full w-full rounded-full object-contain"
              src="{getApiBase()}/beta/api/entity_image?imageId={entity.imageId}"
            />
          </div>
        </ResultLink>
      </div>
    {/if}
    <div class="mb-5 text-xl">
      <ResultLink
        _class="hover:underline"
        href="https://en.wikipedia.org/wiki/{encodeURI(entity.title)}"
      >
        {entity.title}
      </ResultLink>
    </div>
    <div class="text-sm">
      <span><EntitySnippet snippet={entity.smallAbstract} /></span>{' '}
      <span class="italic">
        source:{' '}
        <ResultLink
          _class="text-link visited:text-link-visited hover:underline"
          href="https://en.wikipedia.org/wiki/{encodeURI(entity.title)}"
        >
          wikipedia
        </ResultLink>
      </span>
    </div>
    {#if entity.info.length > 0}
      <div class="mb-2 mt-7 flex w-full flex-col px-4 text-sm">
        <div class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2">
          {#each entity.info as [key, value]}
            <div class="text-neutral">{@html key}</div>
            <div>
              <EntitySnippet snippet={value} />
            </div>
          {/each}
        </div>
      </div>
    {/if}
    {#if entity.relatedEntities.length > 0}
      <div class="mt-5 flex w-full flex-col text-neutral">
        <div class="font-light">Related Searches</div>
        <div class="flex overflow-auto">
          {#each entity.relatedEntities as related (related.title)}
            <div class="flex flex-col items-center p-4">
              {#if related.imageId != null}
                <ResultLink href="/search?q={encodeURIComponent(related.title)}">
                  <div class="h-20 w-20">
                    <img
                      alt="Image of {related.title}"
                      class="h-full w-full rounded-full object-cover"
                      src="{getApiBase()}/beta/api/entity_image?imageId={related.imageId}&maxWidth=200&maxHeight=200"
                    />
                  </div>
                </ResultLink>
              {/if}

              <div class="line-clamp-3 text-center">
                <ResultLink href="/search?q={encodeURI(related.title)}">
                  {related.title}
                </ResultLink>
              </div>
            </div>
          {/each}
        </div>
      </div>
    {/if}
  </div>
</div>
