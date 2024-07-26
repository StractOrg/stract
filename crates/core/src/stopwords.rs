// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use hashbrown::{HashMap, HashSet};
use whatlang::Lang;

macro_rules! include_stopwords {
    ($($file:expr => $lang:expr),*) => {{
        let mut stopwords = HashMap::new();

        $(
            stopwords.insert(
                $lang,
                include_str!($file)
                    .lines()
                    .map(|s| s.to_lowercase())
                    .collect(),
            );
        )*

        stopwords
    }};
}

static STOPWORDS: std::sync::LazyLock<HashMap<Lang, HashSet<String>>> =
    std::sync::LazyLock::new(|| {
        include_stopwords!(
                "../stopwords/Afrikaans.txt" => Lang::Afr,
                "../stopwords/Arabic.txt" => Lang::Ara,
                "../stopwords/Armenian.txt" => Lang::Hye,
                "../stopwords/Azerbaijani.txt" => Lang::Aze,
                "../stopwords/Belarusian.txt" => Lang::Bel,
                "../stopwords/Bengali.txt" => Lang::Ben,
                "../stopwords/Bulgarian.txt" => Lang::Bul,
                "../stopwords/Catalan.txt" => Lang::Cat,
                "../stopwords/Croatian.txt" => Lang::Hrv,
                "../stopwords/Czech.txt" => Lang::Ces,
                "../stopwords/Danish.txt" => Lang::Dan,
                "../stopwords/Dutch.txt" => Lang::Nld,
                "../stopwords/English.txt" => Lang::Eng,
                "../stopwords/Esperanto.txt" => Lang::Epo,
                "../stopwords/Estonian.txt" => Lang::Est,
                "../stopwords/Finnish.txt" => Lang::Fin,
                "../stopwords/French.txt" => Lang::Fra,
                "../stopwords/Georgian.txt" => Lang::Kat,
                "../stopwords/German.txt" => Lang::Deu,
                "../stopwords/Greek.txt" => Lang::Ell,
                "../stopwords/Gujarati.txt" => Lang::Guj,
                "../stopwords/Hebrew.txt" => Lang::Heb,
                "../stopwords/Hindi.txt" => Lang::Hin,
                "../stopwords/Hungarian.txt" => Lang::Hun,
                "../stopwords/Indonesian.txt" => Lang::Ind,
                "../stopwords/Italian.txt" => Lang::Ita,
                "../stopwords/Javanese.txt" => Lang::Jav,
                "../stopwords/Kannada.txt" => Lang::Kan,
                "../stopwords/Korean.txt" => Lang::Kor,
                "../stopwords/Latin.txt" => Lang::Lat,
                "../stopwords/Latvian.txt" => Lang::Lav,
                "../stopwords/Lithuanian.txt" => Lang::Lit,
                "../stopwords/Macedonian.txt" => Lang::Mkd,
                "../stopwords/Malayalam.txt" => Lang::Mal,
                "../stopwords/Marathi.txt" => Lang::Mar,
                "../stopwords/Nepali.txt" => Lang::Nep,
                "../stopwords/Persian.txt" => Lang::Pes,
                "../stopwords/Polish.txt" => Lang::Pol,
                "../stopwords/Portuguese.txt" => Lang::Por,
                "../stopwords/Romanian.txt" => Lang::Ron,
                "../stopwords/Russian.txt" => Lang::Rus,
                "../stopwords/Serbian.txt" => Lang::Srp,
                "../stopwords/Slovak.txt" => Lang::Slk,
                "../stopwords/Slovenian.txt" => Lang::Slv,
                "../stopwords/Spanish.txt" => Lang::Spa,
                "../stopwords/Japanese.txt" => Lang::Jpn
        )
    });

pub fn get(lang: &Lang) -> Option<&'static HashSet<String>> {
    STOPWORDS.get(lang)
}

pub fn all() -> &'static HashMap<Lang, HashSet<String>> {
    &STOPWORDS
}
