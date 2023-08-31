import { Signal } from "@preact/signals";
import { Select } from "../components/Select.tsx";
import { ALL_REGIONS } from "../search/region.ts";
import { Region } from "../search/index.ts";

export const RegionSelector = ({ selectedRegion }: {
  selectedRegion: Signal<Region>;
}) => {
  return (
    <div class="select-region flex h-full flex-col justify-center">
      <Select
        form="searchbar-form"
        class="font-light"
        id="region-selector"
        name="gl"
        onChange={(e) => {
          console.log(e);
          const form = (e.target as HTMLSelectElement).form!;
          form.submit();
        }}
      >
        <option>
          All Languages
        </option>
        {ALL_REGIONS.slice(1).map((region) => (
          <option
            value={region}
            selected={region == selectedRegion.value}
          >
            {region}
          </option>
        ))}
      </Select>
    </div>
  );
};
