import { all, createStarryNight } from "https://esm.sh/@wooorm/starry-night@2";
import { match } from "ts-pattern";
import { Element, Node, Root, Text } from "npm:@types/hast";
import { JSX } from "preact";

const languages = [
  ["ts", /(export\s+(const|function|let))|\b(var|async)\b/g],
  ["tsx", /(export\s+(const|function|let))|\b(var|async)\b|(\/>)/g],
  ["rust", /pub\s+(async\s+)?(struct|enum|fn)/g],
] as const;
const detectLanguage = (code: string) => {
  let best = 0;
  let bestLang = null;
  for (const [name, regex] of languages) {
    const n = Array.from(code.matchAll(regex)).length;
    if (n > best) {
      best = n;
      bestLang = name;
    }
  }
  return bestLang;
};

const starryNight = await createStarryNight(all);

export type CodeProps = {
  lang?: string;
  code: string;
};
export const Code = (
  { code, lang = detectLanguage(code) ?? "js" }: CodeProps,
) => {
  const scope = starryNight.flagToScope(lang)!;
  const root = starryNight.highlight(
    code.trim(),
    scope,
  );

  return (
    <pre><code class="text-gray-600 [&>i]:not-italic"><CodeI node={root} /></code></pre>
  );
};
const CodeI = ({ node }: { node: Node }) =>
  match(node).returnType<JSX.Element | null>()
    .with(
      { type: "root" },
      (n): n is Root => n.type == "root",
      ({ children }) => <>{children.map((c) => <CodeI node={c} />)}</>,
    )
    .with(
      { type: "text" },
      (n): n is Text => n.type == "text",
      (n) => <>{n.value}</>,
    )
    .with(
      { type: "element" },
      (n): n is Element => n.type == "element",
      (n) => {
        const c = match(n.properties.className)
          // .with(["pl-k"], () => "text-purple-700")
          .with(["pl-k"], () => "text-brand")
          // entity
          .with(["pl-en"], () => "text-teal-700")
          .with(["pl-e"], () => "text-emerald-600")
          // entity tag
          .with(["pl-ent"], () => "text-teal-700 italic")
          // constant
          .with(["pl-c1"], () => "text-teal-700")
          // comment
          .with(["pl-c"], () => "text-teal-800/80")
          // string
          .with(["pl-s"], () => "text-green-700")
          .with(["pl-cce"], () => "text-green-500")
          .with(["pl-pds"], () => "")
          // storage-modifier-import
          .with(["pl-smi"], () => "text-sky-800")
          .with(["pl-sr"], () => "text-red-200")
          // variable
          .with(["pl-v"], () => "text-gray-900")
          .with(["pl-pse"], () => "text-gray-400")
          .otherwise((c) => {
            console.log("Unknown class:", c, JSON.stringify(n.children));
            return "";
          });

        const children = n.children.map((c) => <CodeI node={c} />);

        return c ? <span class={c}>{children}</span> : <>{children}</>;
      },
    )
    .otherwise((n) => (
      <span class="italic text-gray-500">Unknown node: {n.type}</span>
    ));

export const TS_SAMPLE = `
export type CodeProps = {
  lang: string;
  code: string;
};
export const Code = ({ lang, code }: CodeProps) => {
  const scope = starryNight.flagToScope(lang)!;
  const root = starryNight.highlight(
    "const app = async () => { await run() }",
    scope,
  );

  return <pre><code><CodeI node={root} /></code></pre>;
};
const CodeI = ({ node }: { node: Node }) =>
  match(node).returnType<JSX.Element | null>()
    .with(
      { type: "root" },
      (n): n is Root => n.type == "root",
      ({ children }) => <>{children.map((c) => <CodeI node={c} />)}</>,
    )
    .with(
      { type: "text" },
      (n): n is Text => n.type == "text",
      (n) => <span>{n.value}</span>,
    )
    .with(
      { type: "element" },
      (n): n is Element => n.type == "element",
      (n) => (
        <span
          class={match(n.properties.className).with(
            ["pl-k"],
            () => "text-brand",
          ).with(
            ["pl-en"],
            () => "text-brand_contrast",
          ).otherwise((c) => {
            console.log("Unknown class:", c);
            return "";
          })}
        >
          {n.children.map((c) => <CodeI node={c} />)}
        </span>
      ),
    )
    .otherwise((n) => (
      <span class="italic text-gray-500">Unknown node: {n.type}</span>
    ));
`;
export const RS_SAMPLE = `
    #[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]
pub struct Optic {
    pub rankings: Vec<RankingCoeff>,
    pub site_rankings: SiteRankings,
    pub rules: Vec<Rule>,
    pub discard_non_matching: bool,
}

impl Optic {
    pub fn parse(optic: &str) -> Result<Self> {
        parse(optic)
    }
}

impl ToString for Optic {
    fn to_string(&self) -> String {
        let mut res = String::new();

        if self.discard_non_matching {
            res.push_str("DiscardNonMatching;\\n");
        }

        for rule in &self.rules {
            res.push_str(&rule.to_string());
        }

        for ranking in &self.rankings {
            res.push_str(&format!("{};\\n", ranking.to_string()));
        }

        res.push_str(&self.site_rankings.to_string());

        res
    }
}
`;
