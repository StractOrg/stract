import { Select } from "../components/Select.tsx";
import { OpticOption, opticsSignal } from "../search/optics.ts";

export const OpticSelector = ({ defaultOptics, searchOnChange, selected }: {
  defaultOptics: OpticOption[];
  searchOnChange: boolean;
  selected?: string;
}) => {
  return (
    <div class="m-0 flex h-full flex-col justify-center p-0">
      <Select
        form="searchbar-form"
        id="optics-selector"
        name="optic"
        class="m-0 font-light"
        onChange={(e) => {
          const form = (e.target as HTMLSelectElement).form;
          if (searchOnChange && form) form.submit();
        }}
        value={selected}
      >
        <option value="">No Optic</option>
        {[...opticsSignal.value.data, ...defaultOptics].map((optic) => (
          <option
            value={optic.url}
            title={optic.description}
          >
            {optic.name}
          </option>
        ))}
      </Select>
    </div>
  );
};
