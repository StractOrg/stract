<script lang="ts">
  export let value: number;
  export let min: number;
  export let max: number;
  export let neutral: number = 0;

  $: value = Math.min(Math.max(value, min), max);

  $: negativeFrag = 0;
  $: positiveFrag = 0;

  $: {
    if (value < neutral) {
      negativeFrag = (value - neutral) / (min - neutral);
      positiveFrag = 0;
    } else {
      negativeFrag = 0;
      positiveFrag = (value - neutral) / (max - neutral);
    }
  }

  const width = 600;
  const height = 30;

  $: maskWidth = positiveFrag > negativeFrag ? width / 2 : (width / 2) * (1 - negativeFrag);
  $: maskX = positiveFrag > negativeFrag ? maskWidth * (1 + positiveFrag) : 0;

  $: otherMaskWidth = width / 2;
  $: otherMaskX = positiveFrag > negativeFrag ? 0 : width / 2;
</script>

<div class="flex w-64">
  {#if value != neutral}
    <svg viewBox="0 0 {width} {height}">
      <defs>
        <linearGradient id="gradient" x1="0" x2="1" y1="1" y2="1">
          <stop offset="0%" stop-color="#eb4334" />
          <stop offset="100%" stop-color="#37eb34" />
        </linearGradient>
      </defs>

      <rect id="rect" {width} {height} rx="15" fill="url(#gradient)" />

      <rect x={maskX} y="0" width={maskWidth} height={height + 1} fill="white" />

      <rect x={otherMaskX} y="0" width={otherMaskWidth} height={height + 1} fill="white" />
    </svg>
  {/if}
</div>
