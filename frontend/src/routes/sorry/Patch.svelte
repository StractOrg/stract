<script lang="ts">
  import Check from '~icons/heroicons/check-20-solid';
  export let id: number;
</script>

<div class="relative float-left flex h-1/3 w-1/3">
  <input
    id={'patch_' + id}
    type="checkbox"
    class="peer hidden"
    checked={false}
    name={`patch[${id}]`}
  />
  <label
    for={'patch_' + id}
    class="h-full w-full hover:cursor-pointer hover:bg-white/30 peer-checked:bg-black/75"
  />
  <Check
    class="pointer-events-none invisible absolute left-1/2 top-1/2 m-0 h-10 w-10 translate-x-[-50%] translate-y-[-50%] text-white peer-checked:visible"
  />
</div>
