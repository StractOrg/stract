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

use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
};

use crate::{
    entity_index::{
        entity::{Entity, Paragraph, Span, WikiNodeExt},
        EntityIndex,
    },
    Result,
};

use bzip2::bufread::MultiBzDecoder;
use itertools::Itertools;
use parse_wiki_text::{Node, Parameter};
use quick_xml::{events::Event, name::QName};

struct EntityIterator<R: BufRead> {
    inside_title: bool,
    inside_text: bool,
    buf: Vec<u8>,
    current_entity: Option<EntityBuilder>,
    reader: quick_xml::Reader<R>,
}

impl<R: BufRead> From<R> for EntityIterator<R> {
    fn from(reader: R) -> Self {
        Self {
            inside_title: false,
            inside_text: false,
            buf: Vec::new(),
            current_entity: None,
            reader: quick_xml::Reader::from_reader(reader),
        }
    }
}

impl<R: BufRead> Iterator for EntityIterator<R> {
    type Item = Entity;

    fn next(&mut self) -> Option<Self::Item> {
        use Event::*;

        loop {
            let event = self.reader.read_event_into(&mut self.buf).ok()?;
            let name = match &event {
                Start(b) | Empty(b) => b.name(),
                End(b) => b.name(),
                _ => QName(&[]),
            };
            match (&event, name.as_ref()) {
                (Start(_), b"page") => self.current_entity = Some(EntityBuilder::new()),
                (End(_), b"page") => {
                    if let Some(entity) = self.current_entity.take().and_then(|e| e.build()) {
                        return Some(entity);
                    }
                }
                (Empty(_), b"redirect") => self.current_entity = None,
                (Start(_), b"title") => self.inside_title = true,
                (End(_), b"title") => self.inside_title = false,
                (Start(_), b"text") => self.inside_text = true,
                (End(_), b"text") => self.inside_text = false,
                (Text(e), _) => {
                    if let Some(entity) = &mut self.current_entity {
                        if self.inside_title {
                            entity.append_title(e.unescape().unwrap().as_ref());
                        } else if self.inside_text {
                            entity.append_text(e.unescape().unwrap().as_ref());
                        }
                    }
                }
                (Eof, _) => break,
                _ => (),
            }
        }

        None
    }
}

pub struct EntityIndexer;

impl EntityIndexer {
    pub fn run(wikipedia_dump_path: String, output_path: String) -> Result<()> {
        let reader = BufReader::new(File::open(wikipedia_dump_path)?);
        let reader = BufReader::new(MultiBzDecoder::new(reader));
        let mut index = EntityIndex::open(output_path)?;
        EntityIterator::from(reader)
            .filter(|entity| !entity.categories.is_empty())
            .take(200_000)
            .for_each(|entity| {
                index.insert(entity);
            });

        index.commit();

        Ok(())
    }
}

#[derive(Debug)]
pub struct EntityBuilder {
    title: String,
    text: String,
}

/// Extract the key-value pairs from the `Infobox` template. Returns `None` if
/// no such template exists.
fn extract_info(nodes: &[Node]) -> Option<BTreeMap<String, Span>> {
    let info_parameters = nodes.iter().find_map(|node| {
        let (name, parameters) = node.as_template()?;
        name.iter()
            .any(|n| n.as_text().is_some_and(|v| v.starts_with("Infobox")))
            .then_some(parameters)
    })?;
    let info = info_parameters
        .iter()
        .filter_map(|Parameter { name, value, .. }| {
            if value.is_empty() {
                return None;
            }
            let key = name.as_ref()?.first()?.as_text()?;
            Some((key.to_string(), Span::from(value.as_slice())))
        })
        .collect();
    Some(info)
}

/// Renders a subset of [Wikipedia
/// Templates](https://en.wikipedia.org/wiki/Wikipedia:TemplateData/Used_templates).
///
/// There are multiple thousands of these templates, each with their own
/// potentially complex implementation. This function is a _best-effort_
/// implementation of a few the most common templates.
fn render_template(name: &str, parameters: &[Parameter]) -> Option<String> {
    Some(match name {
        // https://en.wikipedia.org/wiki/Template:IPAc-en
        // https://en.wiktionary.org/wiki/Template:grc-IPA
        // https://en.wiktionary.org/wiki/Template:fr-IPA
        "IPAc-en" | "IPA-grc" | "IPA-fr" => {
            parameters
                .iter()
                .filter(|p| p.name.is_none())
                .flat_map(|p| &p.value)
                .filter_map(|v| v.as_text())
                .filter(|v| !["pron"].contains(v))
                .join("")
                .replace('_', " ")

            // NOTE: For debugging
            // format!("@@IPA({parameters:?})@@")
        }
        // https://en.wikipedia.org/wiki/Template:Respell
        "respell" => {
            parameters
                .iter()
                .filter(|p| p.name.is_none())
                .flat_map(|p| &p.value)
                .filter_map(|v| v.as_text())
                .join("-")
                .replace('_', " ")

            // NOTE: For debugging
            // format!("@@respell({parameters:?})@@")
        }
        _ => return None,
    })
}

fn extract_text(nodes: &[Node]) -> (Option<Span>, Vec<Paragraph>) {
    let mut paragraphs = Vec::new();

    for node in nodes
        .iter()
        .skip_while(|node| matches!(node, Node::ParagraphBreak { .. }))
    {
        if paragraphs.is_empty() {
            paragraphs.push(Default::default());
        }
        let current_paragraph = paragraphs.last_mut().unwrap();
        match node {
            Node::Heading { nodes, .. } => {
                let title = nodes.iter().filter_map(|node| node.as_text()).join("");

                let title = if title.is_empty() { None } else { Some(title) };

                paragraphs.push(Paragraph {
                    title,
                    content: Span::default(),
                });
            }
            Node::Template {
                name, parameters, ..
            } => {
                const DEBUG_TEMPLATE: bool = false;

                let name = name.first().and_then(|n| n.as_text()).unwrap_or_default();
                if let Some(content) = render_template(name, parameters) {
                    current_paragraph.content.text.push_str(&content)
                } else if DEBUG_TEMPLATE {
                    current_paragraph
                        .content
                        .text
                        .push_str(&format!("@@Template({name:?})@@",))
                }
            }
            node => current_paragraph.content.add_node(node),
        }
    }

    if paragraphs.is_empty() {
        (None, Vec::new())
    } else {
        let page_abstract = paragraphs.remove(0).content;
        (Some(page_abstract), paragraphs)
    }
}

fn extract_categories(nodes: &[Node]) -> HashSet<String> {
    nodes
        .iter()
        .filter_map(|node| {
            let (_, cat) = node.as_category_target()?.split_once(':')?;
            Some(cat.to_string())
        })
        .collect()
}

impl EntityBuilder {
    pub fn new() -> Self {
        EntityBuilder {
            title: String::new(),
            text: String::new(),
        }
    }

    pub fn append_title(&mut self, text: &str) {
        self.title.push_str(text);
    }

    pub fn append_text(&mut self, text: &str) {
        self.text.push_str(text);
    }

    pub fn build(self) -> Option<Entity> {
        let nodes = parse_wiki_text::Configuration::default()
            .parse(&self.text)
            .nodes;

        let categories = extract_categories(&nodes);

        if categories.contains("Disambiguation") {
            return None;
        }

        let mut info = extract_info(&nodes).unwrap_or_default();
        let image = info.remove("image").map(|span| span.text);

        let (Some(page_abstract), paragraphs) = extract_text(&nodes) else {
            return None;
        };

        Some(Entity {
            title: self.title,
            info,
            image,
            page_abstract,
            paragraphs,
            categories,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::entity_index::entity::EntitySnippet;

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
    fn check_abstract(title: &str, text: &str, expect: impl FnOnce(String)) {
        let entity = EntityBuilder {
            title: title.to_string(),
            text: text.to_string(),
        }
        .build()
        .unwrap();

        let categories = entity.categories.iter().sorted().format(", ");
        let info = entity
            .info
            .iter()
            .sorted_by_key(|(k, _)| *k)
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
            format!("Categories: {categories}"),
            format!("Info:\n{info}"),
            "\n---\n".to_string(),
            fmt_span(&entity.page_abstract).trim().to_string(),
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
    fn aristotle() {
        check_abstract(
            "Aristotle",
            include_str!("../../testcases/entity/aristotle.txt"),
            expect!(@r###"
            Title: Aristotle
            Image: Some("Aristotle Altemps Inv8575.jpg")
            Categories: 322 BC deaths, 384 BC births, 4th-century BC mathematicians, 4th-century BC philosophers, 4th-century BC writers, Academic philosophers, Acting theorists, Ancient Greek biologists, Ancient Greek cosmologists, Ancient Greek economists, Ancient Greek epistemologists, Ancient Greek ethicists, Ancient Greek logicians, Ancient Greek mathematicians, Ancient Greek metaphilosophers, Ancient Greek metaphysicians, Ancient Greek philosophers, Ancient Greek philosophers of art, Ancient Greek philosophers of language, Ancient Greek philosophers of mind, Ancient Greek physicists, Ancient Greek political philosophers, Ancient Stagirites, Ancient literary critics, Aphorists, Aristotelian philosophers, Aristotelianism, Aristotle, Attic Greek writers, Critical thinking, Cultural critics, Founders of philosophical traditions, Greek geologists, Greek male writers, Greek meteorologists, Greek social commentators, Humor researchers, Irony theorists, Metic philosophers in Classical Athens, Moral philosophers, Natural philosophers, Ontologists, Peripatetic philosophers, Philosophers and tutors of Alexander the Great, Philosophers of ancient Chalcidice, Philosophers of culture, Philosophers of education, Philosophers of ethics and morality, Philosophers of history, Philosophers of law, Philosophers of literature, Philosophers of logic, Philosophers of love, Philosophers of psychology, Philosophers of science, Philosophers of sexuality, Philosophers of technology, Philosophers of time, Philosophical logic, Philosophical theists, Philosophy academics, Philosophy writers, Rhetoric theorists, Social critics, Social philosophers, Students of Plato, Trope theorists, Virtue ethicists, Virtue ethics, Western culture, Western philosophy, Zoologists
            Info:
             - birth_date: 384 BC
             - birth_place: [Stagira](Stagira_(ancient_city)), [[Chalcidice]]
             - caption: Roman copy in marble of a Greek bronze bust of Aristotle by [[Lysippos]], c. 330 BC, with modern alabaster mantle
             - death_date: 322 BC (aged 61–62)
             - death_place: [[Euboea]], [Macedonian Empire](Macedonia_(ancient_kingdom)#Empire)
             - education: [[Platonic Academy]]
             - era: [[Ancient Greek philosophy]]
             - influenced:
                Virtually all subsequent [Western](Western_philosophy) and [Middle Eastern](Middle_Eastern_philosophy) philosophy, especially [[…

                See: [[List of writers influenced by Aristotle]], [[Commentaries on Aristotle]], [[Pseudo-Aristotle]]
             - influences:
                * [[Plato]]
                * [[Socrates]]
                * [[Heraclitus]]
                * [[Parmenides]]
                * [[Empedocles]]
                * [Phaleas](Phaleas_of_Chalcedon)
                * [Hippodamus](Hippodamus_of_Miletus)
                * [[Hippias]].
             - main_interests:
                * [[Biology]]
                * [[Zoology]]
                * [[Psychology]]
                * [[Physics]]
                * [[Metaphysics]]
                * [[Logic]]
                * [[Ethics]]
                * [[Rhetoric]]
                * [[Aesthetics]]
                * [[Music]]
                * [[Poetry]]
                * [[Economics]]
                * [[Politics]]
                * [[Government]]
                * [[Meteorology]]
                * [[Geology]].
             - name: Aristotle
             - notable_ideas: [[Aristotelianism]]. . .
             - notable_students: [[Alexander the Great]], [[Theophrastus]], [[Aristoxenus]]
             - notable_works: [[Corpus Aristotelicum]]
             - region: [[Western philosophy]]
             - school_tradition:
                * [[Peripatetic school]]
                * [[Aristotelianism]]
                * [[Classical republicanism]].
             - spouse: [[Pythias]]

            ---

            Aristotle (ˈærᵻstɒtəl;  Aristotélēs, aristotélɛːs; 384–322BC) was a Greek [[philosopher]] and [[polymath]] during the [Classical period](Classical_Greece) in [[Ancient Greece]]. Taught by [[Plato]], he was the founder of the [[Peripatetic school]] of philosophy within the [Lyceum](Lyceum_(Classical)) and the wider [Aristotelian](Aristotelianism) tradition. His writings cover many subjects including [physics](Physics_(Aristotle)), [[biology]], [[zoology]], [[metaphysics]], [[logic]], [[ethics]], [[aesthetics]], [poetry](Poetics_(Aristotle)), [[theatre]], [[music]], [[rhetoric]], [[psychology]], [[linguistics]], [[economics]], [[politics]], [[meteorology]], [geology](History_of_geology), and [[government]]. Aristotle provided a complex synthesis of the various philosophies existing prior to him. It was above all from his teachings that [the West](Western_civilization) inherited its intellectual [[lexicon]], as well as problems and methods of inquiry. As a result, his philosophy has exerted a unique influence on almost every form of knowledge in the West and it continues to be a subject of contemporary philosophical discussion.
            Little is known about his life. Aristotle was born in the city of [Stagira](Stagira_(ancient_city)) in [[Northern Greece]]. His father, [Nicomachus](Nicomachus_(father_of_Aristotle)), died when Aristotle was a child, and he was brought up by a guardian. At seventeen or eighteen years of age he joined [[Plato's Academy]] in [[Athens]] and remained there until the age of thirty-seven (). Shortly after Plato died, Aristotle left Athens and, at the request of [[Philip II of Macedon]], tutored [[Alexander the Great]] beginning in 343 BC. He established a library in the [Lyceum](Lyceum_(Classical)) which helped him to produce many of his hundreds of books on [[papyrus]] [[scrolls]]. Though Aristotle wrote many elegant treatises and dialogues for publication, only around [a third of his original output has survived](Corpus_Aristotelicum), none of it intended for publication.
            Aristotle's views profoundly shaped [medieval scholarship](medieval_philosophy). The influence of [physical science](Aristotelian_physics) extended from [[Late Antiquity]] and the [[Early Middle Ages]] into the [[Renaissance]], and were not replaced systematically until [the Enlightenment](Age_of_Enlightenment) and theories such as [[classical mechanics]] were developed. Some of Aristotle's zoological observations found in [his biology](Aristotle's_biology), such as on the [hectocotyl (reproductive) arm](Hectocotylus) of the [[octopus]], were disbelieved until the 19th century. He also influenced [[Judeo-Islamic philosophies (800–1400)]] during the [[Middle Ages]], as well as [[Christian theology]], especially the [[Neoplatonism]] of the [[Early Church]] and the [scholastic](Scholasticism) tradition of the [[Catholic Church]]. Aristotle was revered among medieval Muslim scholars as "The First Teacher", and among medieval Christians like [[Thomas Aquinas]] as simply "The Philosopher", while the poet [Dante](Dante_Alighieri) called him "the master of those who know". His works contain the earliest known formal study of logic, and were studied by medieval scholars such as [[Peter Abelard]] and [[John Buridan]].
            Aristotle's influence on logic continued well into the 19th century. In addition, his [ethics](Aristotelian_ethics), though always influential, gained renewed interest with the modern advent of [[virtue ethics]]. Aristotle has been called "the father of logic", "the father of biology", "the father of political science", "the father of zoology", "the father of embryology", "the father of natural law", "the father of scientific method", "the father of rhetoric", "the father of psychology", "the father of realism", "the father of criticism", "the father of individualism", "the father of teleology", and "the father of meteorology".
            "###),
        );
    }

    #[test]
    fn barack_obama() {
        check_abstract(
            "Barack Obama",
            include_str!("../../testcases/entity/obama.txt"),
            expect!(@r###"
            Title: Barack Obama
            Image: Some("President Barack Obama.jpg")
            Categories: 1961 births, 20th-century African-American academics, 20th-century African-American men, 20th-century American male writers, 20th-century American non-fiction writers, 20th-century Protestants, 21st-century African-American men, 21st-century American male writers, 21st-century American non-fiction writers, 21st-century American politicians, 21st-century Protestants, 21st-century presidents of the United States, 21st-century scholars, Activists from Hawaii, Activists from Illinois, African-American Christians, African-American United States senators, African-American candidates for President of the United States, African-American educators, African-American feminists, African-American lawyers, African-American non-fiction writers, African-American politicians, African-American state legislators in Illinois, American Nobel laureates, American Protestants, American civil rights lawyers, American community activists, American feminist writers, American gun control activists, American legal scholars, American male non-fiction writers, American memoirists, American people of English descent, American people of French descent, American people of German descent, American people of Irish descent, American people of Kenyan descent, American people of Luo descent, American people of Scottish descent, American people of Swiss descent, American people of Welsh descent, American political writers, Articles containing video clips, Barack Obama, Candidates in the 2008 United States presidential election, Candidates in the 2012 United States presidential election, Columbia College (New York) alumni, Democratic Party (United States) presidential nominees, Democratic Party United States senators, Democratic Party presidents of the United States, Grammy Award winners, Harvard Law School alumni, Illinois Democrats, Illinois lawyers, Illinois state senators, LGBT rights activists from the United States, Living people, Male feminists, Members of the American Philosophical Society, Netflix people, Nobel Peace Prize laureates, Obama family, Politicians from Chicago, Politicians from Honolulu, Presidents of the United States, Proponents of Christian feminism, Punahou School alumni, Scholars of constitutional law, Time Person of the Year, United States senators from Illinois, University of Chicago Law School faculty, Writers from Chicago, Writers from Honolulu
            Info:
             - alma_mater: [[Occidental College]] [[Columbia University]] ([BA](Bachelor_of_Arts)) [Harvard University](Harvard_Law_School) ([JD](Juris_Doc…
             - alt: Obama standing with his arms folded and smiling.
             - awards: [List of honors and awards](List_of_honors_and_awards_received_by_Barack_Obama)
             - birth_date: 1961 8 4
             - birth_name: Barack Hussein Obama II
             - birth_place: [[Honolulu]], [[Hawaii]], U.S.
             - caption: Official portrait, 2012
             - children:
                * [Malia](Malia_Obama)
                * [Sasha](Sasha_Obama)
             - district2: [13th](Illinois's_13th_Senate_district)
             - education: [[Punahou School]]
             - jr/sr1: United States Senator
             - occupation: Politician lawyer author
             - office: President of the United States
             - order: 44th
             - parents:
                * [[Barack Obama Sr.]]
                * [[Ann Dunham]]
             - party: [Democratic](Democratic_Party_(United_States))
             - predecessor: [[George W. Bush]]
             - predecessor1: [Peter Fitzgerald](Peter_Fitzgerald_(politician))
             - predecessor2: [Alice Palmer](Alice_Palmer_(politician))
             - relatives: [[Family of Barack Obama]]
             - residence: [[Kalorama (Washington, D.C.)]]
             - signature: Barack Obama signature.svg
             - signature_alt: Cursive signature in ink
             - spouse: [Michelle Robinson](Michelle_Obama) October 3, 1992
             - state1: [[Illinois]]
             - state_senate2: Illinois
             - successor: [[Donald Trump]]
             - successor1: [[Roland Burris]]
             - successor2: [[Kwame Raoul]]
             - term_end: January 20, 2017
             - term_end1: November 16, 2008
             - term_end2: November 4, 2004
             - term_start: January 20, 2009
             - term_start1: January 3, 2005
             - term_start2: January 8, 1997
             - vicepresident: [[Joe Biden]]
             - website: https://barackobama.com Official website https://www.obama.org Obama Foundation obamawhitehouse.archives.gov White House Archive…

            ---

            Barack Hussein Obama II (bəˈrɑːk huːˈseɪn oʊˈbɑːmə bə-RAHK hoo-SAYN oh-BAH-mə; born August 4, 1961) is an American politician who served as the 44th [[president of the United States]] from 2009 to 2017. A member of the [Democratic Party](Democratic_Party_(United_States)), he was the first African-American  president of the United States. Obama previously served as a [[U.S. senator]] from [[Illinois]] from 2005 to 2008 and as an [[Illinois state senator]] from 1997 to 2004.
            Obama was born in [[Honolulu]], [[Hawaii]]. After graduating from [[Columbia University]] in 1983, he worked as a [community organizer](Community_organizing) in [[Chicago]]. In 1988, he enrolled in [[Harvard Law School]], where he was the first black president of the [[Harvard Law Review]]. After graduating, he became a civil rights attorney and an academic, teaching constitutional law at the [[University of Chicago Law School]] from 1992 to 2004. Turning to elective politics, he [represented the 13th district](Illinois_Senate_career_of_Barack_Obama) in the [[Illinois Senate]] from 1997 until 2004, when he [ran for the U.S. Senate](2004_United_States_Senate_election_in_Illinois). Obama received national attention in 2004 with his March Senate primary win, his well-received July [Democratic National Convention](2004_Democratic_National_Convention) [keynote address](2004_Democratic_National_Convention_keynote_address), and his landslide November election to the Senate. In 2008, a year after beginning [his campaign](Barack_Obama_2008_presidential_campaign), and after [a close primary campaign](2008_Democratic_Party_presidential_primaries) against [[Hillary Clinton]], he was nominated by the Democratic Party for president. Obama was elected over [Republican](Republican_Party_(United_States)) nominee [[John McCain]] in the [general election](2008_United_States_presidential_election) and was [inaugurated](First_inauguration_of_Barack_Obama) alongside his running mate [[Joe Biden]], on January 20, 2009. Nine months later, he was named the [[2009 Nobel Peace Prize]] laureate, a decision that drew a mixture of praise and criticism.
            Obama signed many landmark bills into law during his first two [years in office](Presidency_of_Barack_Obama). The main reforms include: the [[Affordable Care Act]] (ACA or "Obamacare"), although without a [[public health insurance option]]; the [[Dodd–Frank Wall Street Reform and Consumer Protection Act]]; and the [[Don't Ask, Don't Tell Repeal Act of 2010]]. The [American Recovery and Reinvestment Act](American_Recovery_and_Reinvestment_Act_of_2009) and [Tax Relief, Unemployment Insurance Reauthorization, and Job Creation Act](Tax_Relief,_Unemployment_Insurance_Reauthorization,_and_Job_Creation_Act_of_2010) served as economic stimuli amidst the [Great Recession](Great_Recession_in_the_United_States). After a [lengthy debate over the national debt limit](United_States_debt-ceiling_crisis_of_2011), he signed the [Budget Control](Budget_Control_Act_of_2011) and the [American Taxpayer Relief Acts](American_Taxpayer_Relief_Act_of_2012). In foreign policy, he increased U.S. troop levels in [Afghanistan](War_in_Afghanistan_(2001–present)), reduced nuclear weapons with the United States–[[Russia]] [[New START]] treaty, and [ended military involvement](Withdrawal_of_U.S._troops_from_Iraq_(2007–2011)) in the [[Iraq War]]. In 2011, Obama controversially ordered the drone-strike killing of [[Anwar al-Awlaki]], a US citizen and suspected [[al-Qaeda]] operative. He ordered [military involvement in Libya](2011_military_intervention_in_Libya) for the implementation of the [UN Security Council Resolution 1973](United_Nations_Security_Council_Resolution_1973), contributing to the overthrow of [[Muammar Gaddafi]]. He also ordered the [military operation](Killing_of_Osama_bin_Laden#Operation_Neptune_Spear) that resulted in the death of [[Osama bin Laden]].
            After winning [re-election](2012_United_States_presidential_election) by defeating Republican opponent [[Mitt Romney]], Obama was [sworn in for a second term](Second_inauguration_of_Barack_Obama) on January 20, 2013. During this term, he condemned the [2013 Snowden leaks](Global_surveillance_disclosures_(2013–present)) as unpatriotic, but called for more restrictions on the [[National Security Agency]] (NSA) to address privacy issues. Obama also promoted inclusion for [LGBT Americans](LGBT_American). His administration filed briefs that urged the [Supreme Court](Supreme_Court_of_the_United_States) to strike down [same-sex marriage](Same-sex_marriage_in_the_United_States) bans as unconstitutional ([[United States v. Windsor]] and [[Obergefell v. Hodges]]); same-sex marriage was [legalized](Same-sex_marriage_in_the_United_States) nationwide in 2015 after the Court ruled so in Obergefell. He advocated for [gun control](Gun_politics_in_the_United_States) in response to the [[Sandy Hook Elementary School shooting]], indicating support for a ban on [[assault weapons]], and issued wide-ranging executive actions concerning [[global warming]] and immigration. In foreign policy, he ordered [military interventions in Iraq](American-led_intervention_in_Iraq_(2014–present)) and [Syria](American-led_intervention_in_the_Syrian_civil_war) in response to gains made by [ISIL](Islamic_State) after the 2011 withdrawal from Iraq, promoted discussions that led to the 2015 [[Paris Agreement]] on global climate change, drew down [U.S. troops in Afghanistan](Withdrawal_of_United_States_troops_from_Afghanistan_(2011–2016)) in 2016, initiated [sanctions against Russia](International_sanctions_during_the_Ukrainian_crisis) following its [annexation of Crimea](Annexation_of_Crimea_by_the_Russian_Federation) and again after [interference in the 2016 U.S. elections](Russian_interference_in_the_2016_United_States_elections), brokered the [[Joint Comprehensive Plan of Action]] nuclear deal with Iran, and [normalized U.S. relations with Cuba](Cuban_thaw). Obama nominated [three justices to the Supreme Court](Barack_Obama_Supreme_Court_candidates): [[Sonia Sotomayor]] and [[Elena Kagan]] were confirmed as justices, while [[Merrick Garland]] was [denied hearings or a vote](Merrick_Garland_Supreme_Court_nomination) from the Republican-majority [Senate](United_States_Senate). Obama left office on January 20, 2017, and continues to reside in [[Washington, D.C.]]
            During Obama's [terms as president](Presidency_of_Barack_Obama), the United States' reputation abroad, as well as the American economy, significantly improved. Scholars and historians rank him among [the upper to mid tier](Historical_rankings_of_presidents_of_the_United_States) of American presidents. Since leaving office, Obama has remained active in Democratic politics, including campaigning for candidates in the [2018 midterm elections](2018_United_States_elections), appearing at the [[2020 Democratic National Convention]] and campaigning for Biden during the [2020 presidential election](2020_United_States_presidential_election). Outside of politics, Obama has published three [bestselling books](Bibliography_of_Barack_Obama): [[Dreams from My Father]] (1995), [[The Audacity of Hope]] (2006) and [[A Promised Land]] (2020).
            "###),
        );
    }

    #[test]
    fn algorithm() {
        check_abstract(
            "Algorithm",
            include_str!("../../testcases/entity/algorithm.txt"),
            expect!(@r###"
            Title: Algorithm
            Image: None
            Categories: Algorithms, Articles with example pseudocode, Mathematical logic, Theoretical computer science
            Info:


            ---

            In [[mathematics]] and [[computer science]], an algorithm (ˈælɡərɪðəm) is a finite sequence of [[rigorous]] instructions, typically used to solve a class of specific [problems](Computational_problem)s or to perform a [[computation]]. Algorithms are used as specifications for performing [calculations](calculation) and [[data processing]]. By making use of [[artificial intelligence]], algorithms can perform automated deductions (referred to as [[automated reasoning]]) and use mathematical and logical tests to divert the code execution through various routes (referred to as [[automated decision-making]]). Using human characteristics as descriptors of machines in metaphorical ways was already practiced by [[Alan Turing]] with terms such as "memory", "search" and "stimulus".
            In contrast, a [heuristic](Heuristic_(computer_science)) is an approach to problem solving that may not be fully specified or may not guarantee correct or optimal results, especially in problem domains where there is no well-defined correct or optimal result.
            As an [[effective method]], an algorithm can be expressed within a finite amount of space and time, and in a well-defined formal language for calculating a [function](Function_(mathematics)). Starting from an initial state and initial input (perhaps [empty](Empty_string)), the instructions describe a [[computation]] that, when [executed](Execution_(computing)), proceeds through a finite number of well-defined successive states, eventually producing "output" and terminating at a final ending state. The transition from one state to the next is not necessarily [[deterministic]]; some algorithms, known as [[randomized algorithms]], incorporate random input.
            "###),
        );
    }

    #[test]
    fn andre() {
        check_abstract(
            "Andre",
            include_str!("../../testcases/entity/andre.txt"),
            expect!(@r###"
            Title: Andre
            Image: Some("Andre Agassi (2011).jpg")
            Categories: 1970 births, 20th-century American businesspeople, 21st-century American businesspeople, ATP number 1 ranked singles tennis players, American autobiographers, American investors, American male tennis players, American people of Armenian descent, American people of Iranian descent, American people of Iranian-Assyrian descent, American real estate businesspeople, American sportspeople in doping cases, Andre Agassi, Armenian-American tennis players, Assyrian sportspeople, Australian Open (tennis) champions, Doping cases in tennis, Ethnic Armenian sportspeople, French Open champions, Grand Slam (tennis) champions in men's singles, ITF World Champions, International Tennis Hall of Fame inductees, Iranian Assyrian people, Iranian people of Armenian descent, Living people, Medalists at the 1996 Summer Olympics, Nevada Democrats, Novak Djokovic coaches, Olympic gold medalists for the United States in tennis, Philanthropists from Nevada, Sportspeople from Las Vegas, Sportspeople of Iranian descent, Steffi Graf, Tennis people from Nevada, Tennis players at the 1996 Summer Olympics, US Open (tennis) champions, Wimbledon champions, Writers from Las Vegas
            Info:
             - AustralianOpenresult: W ([1995](1995_Australian_Open_–_Men's_singles), [2000](2000_Australian_Open_–_Men's_singles), [2001](2001_Australian_Open_–_Men…
             - CoachPlayers: [[Novak Djokovic]] (2017–2018) [[Grigor Dimitrov]] (2018–2020)
             - CoachYears: 2017–2020
             - DavisCupresult: W (1990, 1992, 1995)
             - FrenchOpenDoublesresult: QF (1992)
             - FrenchOpenresult: W ([1999](1999_French_Open_–_Men's_singles))
             - MastersCupresult: W ([1990](1990_ATP_Tour_World_Championships_–_Singles))
             - Olympicsresult:  ([1996](Tennis_at_the_1996_Summer_Olympics_–_Men's_singles))
             - Othertournaments: Yes
             - Team: yes
             - USOpenDoublesresult: 1R (1987)
             - USOpenresult: W ([1994](1994_US_Open_–_Men's_singles), [1999](1999_US_Open_–_Men's_singles))
             - Wimbledonresult: W ([1992](1992_Wimbledon_Championships_–_Men's_singles))
             - birth_date: 1970 04 29
             - birth_place: Las Vegas, Nevada, U.S.
             - caption: Agassi at the 2011 Champions Shootout
             - careerprizemoney:
                $31,152,975
                *[11thall-timeinearnings](Open_Era_tennis_records_–_men's_singles#Prize_money)
             - coach: [[Emmanuel Agassi]] (1970–83) [[Nick Bollettieri]] (1983–1993) [[Pancho Segura]] (1993) [[Brad Gilbert]] (1994–2002) [[Darren Ca…
             - country:
             - doublesrecord: 40–42
             - doublestitles: 1
             - fullname: Andre Kirk Agassi
             - height:
             - highestdoublesranking: No. 123 (August 17, 1992)
             - highestsinglesranking: [No. 1](List_of_ATP_number_1_ranked_singles_players) (April 10, 1995)
             - medaltemplates: Olympic Games – [Tennis](Tennis_at_the_Summer_Olympics).
             - medaltemplates-expand: yes
             - module:
             - name: Andre Agassi
             - plays: Right-handed (two-handed backhand)
             - residence: [[Las Vegas, Nevada]], U.S.
             - retired: 2006
             - singlesrecord:
             - singlestitles: 60
             - tennishofid: andre-agassi
             - tennishofyear: 2011
             - turnedpro: 1986

            ---

            Andre Kirk Agassi (ˈæɡəsi AG-ə-see; born April 29, 1970) is an American former [world No. 1](List_of_ATP_number_1_ranked_singles_players) tennis player. He is an eight-time [major](Grand_Slam_(tennis)#Tournaments) champion and an [Olympic gold medalist](Tennis_at_the_1996_Summer_Olympics_–_Men's_singles), as well as a runner-up in seven other majors. Agassi is widely considered one of the greatest tennis players of all time.
            Agassi is the second of five men to achieve the [career Grand Slam](Grand_Slam_(tennis)#Career_Grand_Slam) in the [[Open Era]] and the fifth of eight overall to make the achievement. He is also the first of two men to achieve the career Golden Slam (career Grand Slam and [Olympic gold medal](Tennis_at_the_Olympics)), as well as the only man to win a [[career Super Slam]] (career Grand Slam, plus the Olympic gold medal and the [year-end championships](ATP_Finals)).
            Agassi was the first man to win all four singles majors on three different surfaces ([hard](Hardcourt), [clay](Clay_court) and [grass](Grass_court)), and remains the most recent American man to win the [[French Open]] (in [1999](1999_French_Open)) and the [[Australian Open]] (in [2003](2003_Australian_Open)). He also won 17 [Masters](ATP_Tour_Masters_1000) titles and was part of the winning [[Davis Cup]] teams in [1990](1990_Davis_Cup), [1992](1992_Davis_Cup) and [1995](1995_Davis_Cup). Agassi reached the world No. 1 ranking for the first time in 1995, but was troubled by personal issues during the mid-to-late 1990s and sank to No. 141 in 1997, prompting many to believe that his career was over. Agassi returned to No. 1 in 1999 and enjoyed the most successful run of his career over the next four years. During his 20-plus year tour career, Agassi was known by the nickname "The Punisher".
            After suffering from [[sciatica]] caused by two bulging discs in his back, a [[spondylolisthesis]] ([vertebral](vertebra) displacement) and a [[bone spur]] that interfered with the [[nerve]], Agassi retired from professional tennis on September 3, 2006, after losing in the third round of the [US Open](2006_US_Open_–_Men's_singles). He is the founder of the Andre Agassi Charitable Foundation, which has raised over $60million for at-risk children in Southern Nevada. In 2001, the Foundation opened the Andre Agassi College Preparatory Academy in Las Vegas, a K–12 public charter school for at-risk children. He has been married to fellow tennis player [[Steffi Graf]] since 2001.
            "###),
        )
    }

    #[test]
    fn skip_disambiguation_pages() {
        assert!(EntityBuilder {
            title: "Test".to_string(),
            text: include_str!("../../testcases/entity/disambiguation.txt").to_string(),
        }
        .build()
        .is_none());
    }
}