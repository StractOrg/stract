<script lang="ts">
    export let text: string;

    type TextOrBreak =
  | {
      type: 'text';
      value: string;
    }
  | {
      type: 'break';
      value: string;
    };


    // split text into a stream of text and break elements
    $: paragraphs = text
        .split('\n')
        .map((line) => line.trim())
        .filter((line) => line.length > 0)
        .flatMap((line) => {
            return [
                {
                    type: 'text',
                    value: line,
                },
                {
                    type: 'break',
                    value: '\n',
                },
            ];
        }) as TextOrBreak[];
    
    // remove the last break
    $: paragraphs.pop();
    
</script>

{#each paragraphs as paragraph}
    {#if paragraph.type == 'text'}
        {paragraph.value}
    {:else if paragraph.type == 'break'}
        <br />
        <br />
    {/if}
{/each}
