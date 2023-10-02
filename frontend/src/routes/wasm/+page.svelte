<script lang="ts">
  import { browser } from '$app/environment';

  async function bufferToBase64(buffer: BlobPart) {
    const base64url = await new Promise<string>((r) => {
      const reader = new FileReader();
      reader.onload = () => r(reader.result as string);
      reader.readAsDataURL(new Blob([buffer]));
    });
    return base64url.slice(base64url.indexOf(',') + 1);
  }

  const formats = [
    ['Png', 'png'],
    ['Gif', 'gif'],
    ['Ico', 'ico'],
    ['Bmp', 'bmp'],
    ['Farbfeld', 'farbfeld'],
    ['Tga', 'tga'],
    ['OpenExr', 'openexr'],
    ['Tiff', 'tiff'],
    ['Avif', 'avif'],
    ['Qoi', 'qoi'],
  ] as const;

  let format: (typeof formats)[number] = formats[0];
  let files: FileList | undefined;
  let converted: string | undefined;

  $: if (browser && files && files.length > 0) {
    const file = files.item(0)!;
    (async () => {
      const wasm = await import('wasm');
      const buffer = await file.arrayBuffer();
      console.log('converting...');
      const bytes = wasm.convert_image(new Uint8Array(buffer), wasm.TargetFormat[format[0]]);
      converted = `data:img/${format[1]};base64,` + (await bufferToBase64(bytes));
    })();
  }
</script>

<div class="grid place-items-center">
  <div class="flex w-96 flex-col items-end gap-4 rounded border bg-base-200 p-5 shadow">
    <input type="file" bind:files />
    <select class="bg-transparent" bind:value={format}>
      {#each formats as format}
        <option value={format}>{format[0]}</option>
      {/each}
    </select>
    <img alt="Image in {format[0]}" download="image.{format[1]}" src={converted} />
  </div>
</div>
