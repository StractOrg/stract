<script lang="ts">
  export let audioBase64: string;

  $: playing = false;

  let audio: HTMLAudioElement | undefined;
  let button: HTMLButtonElement | undefined;

  $: {
    if (button) {
      if (playing) {
        button.innerText = 'PAUSE';
      } else {
        button.innerText = 'PLAY';
      }
    }
  }

  const click = () => {
    if (audio == undefined || button == undefined) {
      return;
    }

    playing = !playing;

    if (playing) {
      audio.play();
    } else {
      audio.pause();
    }
  };
</script>

<div>
  <p class="text-xl">Write the numbers from the audio file below</p>
  <audio bind:this={audio} src={audioBase64} on:ended={() => (playing = false)}>
    <source src={audioBase64} />
  </audio>
  <button
    type="button"
    class="h-16 w-full rounded bg-primary text-xl font-semibold text-primary-content"
    on:click={click}
    bind:this={button}>PLAY</button
  >
</div>
