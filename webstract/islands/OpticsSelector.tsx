import { Select } from "../components/Select.tsx";
import { OpticOption, opticsSignal } from "../search/optics.ts";

export const OpticSelector = ({ defaultOptics, searchOnChange }: {
  defaultOptics: OpticOption[];
  searchOnChange: boolean;
}) => {
  return (
    <div class="m-0 flex h-full flex-col justify-center p-0">
      <Select
        form="searchbar-form"
        id="optics-selector"
        name="optic"
        class="m-0"
        onChange={(e) => {
          const form = (e.target as HTMLSelectElement).form;
          if (searchOnChange && form) form.submit();
        }}
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
