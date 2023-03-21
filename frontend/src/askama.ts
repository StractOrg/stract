import { faker } from "@faker-js/faker";

type Component = (() => any) | any;

const prepare = (c: Component | null) => (typeof c == "function" ? c() : c);

const passthrough = import.meta.env.DEV;

const fake = (prod: any, dev: any) =>
  passthrough ? faker.helpers.fake(dev) : prod;
const match = (expr: string, cases: Record<string, Component>) => {
  if (passthrough) {
    const options = Object.values(cases);
    return prepare(options[Math.floor(Math.random() * options.length)]);
  }

  return [
    `{% match ${expr} %}`,
    Object.entries(cases).map(([c, v]) => [`{% when ${c} %}`, prepare(v)]),
    ,
    `{% endmatch %}`,
  ].flat(100);
};
type ArrayPattern<S> = S extends `${infer _} in ${infer _}` ? S : never;
const for_ = <S extends string>(pat: ArrayPattern<S>, f: Component) => {
  if (passthrough) {
    const n = Math.floor(Math.random() * 6);
    return Array.from({ length: n })
      .map(() => prepare(f))
      .flat(100);
  }
  return [`{% for ${pat} %}`, prepare(f), `{% endfor %}`].flat(100);
};
const if_ = (pat: string, then: Component, else_: null | Component = null) => {
  if (passthrough) {
    return Math.random() > 0.5 ? prepare(then) : prepare(else_);
  }

  return [
    `{% if ${pat} %}`,
    prepare(then),
    else_ && [`{% else %}`, prepare(else_)],
    `{% endif %}`,
  ].flat(100);
};

export const askama = Object.assign(
  (input: TemplateStringsArray) => {
    const str = input.raw.join("");
    const segments = str.split("$");
    if (passthrough && segments.length > 1) {
      return faker.helpers.fake(segments.slice(1).join("$"));
    } else {
      return `{{ ${segments[0]} }}`;
    }
  },
  { fake, match, for_, if_ }
);
