type Component = (() => any) | any;

const prepare = (c: Component | null) => (typeof c == "function" ? c() : c);

const match = (expr: string, cases: Record<string, Component>) =>
  [
    `{% match ${expr} %}`,
    Object.entries(cases).map(([c, v]) => [`{% when ${c} %}`, prepare(v)]),
    `{% endmatch %}`,
  ].flat(100);
type ArrayPattern<S> = S extends `${infer _} in ${infer _}` ? S : never;
const for_ = <S extends string>(pat: ArrayPattern<S>, f: Component) =>
  [`{% for ${pat} %}`, prepare(f), `{% endfor %}`].flat(100);
const if_ = (pat: string, then: Component, else_: null | Component = null) =>
  [
    `{% if ${pat} %}`,
    prepare(then),
    else_ && [`{% else %}`, prepare(else_)],
    `{% endif %}`,
  ].flat(100);

export const askama = Object.assign(
  (s: TemplateStringsArray) => `{{ ${s.raw.join("")} }}`,
  { match, for_, if_ }
);
