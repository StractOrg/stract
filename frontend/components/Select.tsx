import { twMerge } from "tailwind-merge";
import { JSX } from "preact";

export const Select = (
  { style: _, "class": c, ...rest }: JSX.HTMLAttributes<
    HTMLSelectElement
  >,
) => {
  const svgThingy =
    `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='currentColor'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M19 9l-7 7-7-7' /%3E%3C/svg%3E")`;

  return (
    <select
      class={twMerge("py-0 rounded pl-1 pr-5", c?.toString())}
      style={{
        backgroundColor: `rgba(0, 0, 0, 0)`,
        backgroundImage: svgThingy,
        backgroundPosition: `right`,
        backgroundRepeat: `no-repeat`,
        textAlign: `right`,
        cursor: `pointer`,
        fontSize: `0.9rem`,
        backgroundSize: `1em 1em`,
        border: `none`,
        appearance: `none`,
      }}
      {...rest}
    />
  );
};
