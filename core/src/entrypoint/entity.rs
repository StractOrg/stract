// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use kuchiki::{traits::TendrilSink, NodeRef};
use zimba::{Article, ArticleIterator, Zim};

use crate::{
    entity_index::{
        entity::{Entity, Span},
        EntityIndex,
    },
    Result,
};

struct EntityIterator<'a> {
    articles: ArticleIterator<'a>,
}

impl<'a> EntityIterator<'a> {
    pub fn new(zim: &'a Zim) -> Result<EntityIterator<'a>> {
        Ok(Self {
            articles: zim.articles()?,
        })
    }
}

fn article_to_entity(article: Article) -> Entity {
    let root = kuchiki::parse_html().one(article.content);

    let title = root
        .select_first("title")
        .map(|title| title.text_contents().trim().to_string())
        .unwrap_or(article.title);

    let mut image = root
        .select_first(".infobox")
        .and_then(|infobox| infobox.as_node().select_first("img"))
        .map(|img| img.attributes.borrow().get("src").unwrap().to_string())
        .and_then(|src| src.split('/').last().map(|s| s.to_string()));

    if image.is_none() {
        image = root
            .select_first(".thumbinner")
            .and_then(|figure| figure.as_node().select_first("img"))
            .map(|img| img.attributes.borrow().get("src").unwrap().to_string())
            .and_then(|src| src.split('/').last().map(|s| s.to_string()));
    }

    let info: Vec<_> = root
        .select_first(".infobox")
        .map(|infobox| {
            infobox
                .as_node()
                .select("tr")
                .unwrap()
                .filter_map(|tr| {
                    let mut tds = tr.as_node().select("td").unwrap();
                    let mut key = tds.next()?.text_contents().trim().to_string();

                    while key.is_empty() {
                        key = tds.next()?.text_contents().trim().to_string();
                    }
                    key = key.trim_end_matches(':').to_string();

                    let value = node_into_span(tds.next()?.as_node());
                    Some((key, value))
                })
                .collect()
        })
        .unwrap_or_default();

    let page_abstract = root
        .select("p")
        .unwrap()
        .find(|p| p.text_contents().trim().len() > 10)
        .map(|n| node_into_span(n.as_node()))
        .unwrap_or_default();

    Entity {
        title,
        page_abstract,
        image,
        info,
    }
}

impl<'a> Iterator for EntityIterator<'a> {
    type Item = Entity;

    fn next(&mut self) -> Option<Self::Item> {
        let mut article = self.articles.next()?;

        if article.url == "index" || article.title == "Main Page" {
            article = self.articles.next()?;
        }

        Some(article_to_entity(article))
    }
}

fn node_into_span(node: &NodeRef) -> Span {
    let mut span = Span::default();

    for child in node.children() {
        if let Some(text) = child.as_text() {
            let text = text
                .borrow()
                .as_str()
                .trim_matches(|c| c == '\n' || c == '\r' || c == '\t')
                .to_string();
            span.add_text(text.as_str());
        } else if let Some(elem) = child.as_element() {
            match elem.name.local.as_ref() {
                "a" => {
                    let href = elem.attributes.borrow().get("href").unwrap().to_string();
                    let text = child.text_contents();
                    span.add_link(&text, href);
                }
                "b" | "i" | "p" | "span" => {
                    let text = child.text_contents();
                    let text = text
                        .trim_matches(|c| c == '\n' || c == '\r' || c == '\t')
                        .to_string();
                    span.add_text(text.as_str());
                }
                "ul" | "ol" | "li" | "div" => {
                    let child_span = node_into_span(&child);
                    span.merge(child_span);
                    span.add_text(" ");
                }
                _ => {}
            }
        }
    }

    span.trim_end();

    span
}

pub struct EntityIndexer;

impl EntityIndexer {
    pub fn run(wikipedia_dump_path: String, output_path: String) -> Result<()> {
        let zim = Zim::open(wikipedia_dump_path)?;
        let mut index = EntityIndex::open(output_path)?;
        index.prepare_writer();

        for entity in EntityIterator::new(&zim)? {
            index.insert(entity);
        }

        index.commit();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::entity_index::entity::EntitySnippet;
    use itertools::Itertools;

    use super::*;

    #[macro_export]
    macro_rules! expect {
        ($($t:tt)*) => {
            |res| ::insta::assert_display_snapshot!(res, $($t)*)
        };
    }

    fn fmt_span(s: &Span) -> String {
        EntitySnippet::from_span(s, usize::MAX).to_md(Some("https://en.wikipedia.org/wiki/"))
    }

    fn ellipsis(s: &str) -> String {
        let (n, _) = s.char_indices().nth(128).unwrap_or((s.len(), '.'));

        if s.len() > n {
            format!("{}…", &s[0..n])
        } else {
            s.to_string()
        }
    }

    /// `expect` assert properties of the rendered version of the provided wiki
    /// data.
    fn check_abstract(title: &str, url: &str, content: &str, expect: impl FnOnce(String)) {
        let article = Article {
            url: url.to_string(),
            title: title.to_string(),
            content: content.to_string(),
        };

        let entity = article_to_entity(article);

        let info = entity
            .info
            .iter()
            .map(|(k, v)| {
                let value = fmt_span(v);
                let (sep, text) = if value.contains('\n') {
                    let fmt_value = value
                        .trim_start()
                        .lines()
                        .map(|l| format!("    {}", ellipsis(l)))
                        .format("\n");
                    ("\n", fmt_value.to_string())
                } else {
                    (" ", ellipsis(&value))
                };
                format!(" - {k}:{sep}{text}")
            })
            .format("\n");

        let sections = [
            format!("Title: {}", entity.title),
            format!("Image: {:?}", entity.image),
            format!("Info:\n{info}"),
            "\n---\n".to_string(),
            fmt_span(&entity.page_abstract).split_whitespace().join(" "),
        ];

        expect(
            sections
                .into_iter()
                .join("\n")
                .lines()
                .map(|l| l.trim_end())
                .join("\n"),
        )
    }

    #[test]
    fn extinction() {
        check_abstract(
            "taceousâ\u{80}\u{93}Paleogene extinction event",
            "Cretaceousâ\u{80}\u{93}Paleogene_extinction_event",
            include_str!("../../testcases/entity/extinction.html"),
            expect!(@r###"
            Title: Cretaceous–Paleogene extinction event
            Image: Some("Impact_event.jpg.webp")
            Info:

            
            ---

            The Cretaceous–Paleogene (K–Pg) extinction event, also known as the Cretaceous–Tertiary (K–T) extinction, was a sudden mass extinction of three-quarters of the [plant](Plant) and [animal](Animal) species on Earth, approximately 66 million years ago. The event caused the extinction of all non-avian [dinosaurs](Dinosaur). Most other tetrapods weighing more than 25 kilograms (55 pounds) also became extinct, with the exception of some ectothermic species such as sea turtles and crocodilians. It marked the end of the Cretaceous period, and with it the Mesozoic era, while heralding the beginning of the Cenozoic era, which continues to this day.
            "###),
        );
    }

    #[test]
    fn eukaryote() {
        check_abstract(
            "Eukaryote",
            "Eukaryote",
            include_str!("../../testcases/entity/eukaryote.html"),
            expect!(@r###"
            Title: Eukaryote
            Image: Some("Rhodomonas_salina_CCMP_322.jpg.webp")
            Info:
             - Domain: [Eukaryota](Eukaryote)

            ---

            The eukaryotes (/juːˈkærioʊts, -əts/) constitute the domain of Eukarya, organisms whose cells have a membrane-bound nucleus. All [animals](Animal), [plants](Plant), [fungi](Fungus), and many unicellular organisms are eukaryotes. They constitute a major group of life forms alongside the two groups of prokaryotes: the Bacteria and the Archaea. Eukaryotes represent a small minority of the number of organisms, but due to their generally much larger size, their collective global biomass is much larger than that of prokaryotes.
            "###),
        );
    }

    #[test]
    fn animal() {
        check_abstract(
            "Animal",
            "Animal",
            include_str!("../../testcases/entity/animal.html"),
            expect!(@r###"
            Title: Animal
            Image: Some("Animal_diversity_b.png.webp")
            Info:
             - Domain: [Eukaryota](Eukaryote)
             - Clade: Amorphea
             - Clade: Obazoa
             - (unranked): Opisthokonta
             - (unranked): Holozoa
             - (unranked): Filozoa
             - Kingdom: [Animalia](Animal)
            
            ---
            
            Animals are multicellular, [eukaryotic](Eukaryotic) organisms in the biological kingdom Animalia. With few exceptions, animals consume organic material, breathe oxygen, have myocytes and are able to move, can reproduce sexually, and grow from a hollow sphere of cells, the blastula, during embryonic development.
            "###),
        )
    }

    #[test]
    fn world_heritage() {
        check_abstract(
            "World Heritage Site",
            "World_Heritage_Site",
            include_str!("../../testcases/entity/world_heritage_site.html"),
            expect!(@r###"
            Title: World Heritage Site
            Image: Some("Placa_conmemorativa_de_la_inscripci%C3%B3n_en_la_Lista_del_Patrimonio_Mundial_del_Parque_Nacional_de_Do%C3%B1ana.jpg.webp")
            Info:
            
            
            ---
            
            A World Heritage Site is a landmark or area with legal protection by an international convention administered by the United Nations Educational, Scientific and Cultural Organization (UNESCO). World Heritage Sites are designated by UNESCO for having cultural, historical, scientific or other forms of significance. The sites are judged to contain "cultural and natural heritage around the world considered to be of outstanding value to [humanity](Human)".
            "###),
        )
    }

    #[test]
    fn lion() {
        check_abstract(
            "Lion",
            "Lion",
            include_str!("../../testcases/entity/lion.html"),
            expect!(@r###"
            Title: Lion
            Image: Some("Lion_waiting_in_Namibia.jpg.webp")
            Info:
             - Domain: [Eukaryota](Eukaryote)
             - Kingdom: [Animalia](Animal)
             - Phylum: Chordata
             - Class: Mammalia
             - Order: Carnivora
             - Suborder: Feliformia
             - Family: Felidae
             - Subfamily: Pantherinae
             - Genus: Panthera
             - Species:                                                                                             P. leo[2]
            
            ---
            
            The lion (Panthera leo) is a large cat of the genus Panthera native to Africa and India. It has a muscular, broad-chested body; short, rounded head; round ears; and a hairy tuft at the end of its tail. It is sexually dimorphic; adult male lions are larger than females and have a prominent mane. It is a social species, forming groups called prides. A lion's pride consists of a few adult males, related females, and cubs. Groups of female lions usually hunt together, preying mostly on large ungulates. The lion is an apex and keystone predator; although some lions scavenge when opportunities occur and have been known to hunt [humans](Human), lions typically do not actively seek out and prey on humans.
            "###),
        )
    }

    #[test]
    fn zim() {
        if !Path::new("../data/test.zim").exists() {
            return;
        }

        let zim = Zim::open("../data/test.zim").unwrap();
        let mut it = EntityIterator::new(&zim).unwrap();

        let entity = it.next().unwrap();

        assert_eq!(entity.title, "Animal");
    }
}
