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

use publicsuffix::Psl;

pub static LIST: once_cell::sync::Lazy<publicsuffix::List> = once_cell::sync::Lazy::new(|| {
    include_str!("../../public_suffix_list.dat")
        .parse()
        .expect("Failed to parse public suffix list")
});

pub trait UrlExt {
    fn root_domain(&self) -> Option<&str>;

    fn subdomain(&self) -> Option<&str>;
}

impl UrlExt for url::Url {
    fn root_domain(&self) -> Option<&str> {
        let host = self.host_str()?;
        let suffix = std::str::from_utf8(LIST.domain(host.as_bytes())?.as_bytes()).ok()?;
        Some(suffix)
    }

    fn subdomain(&self) -> Option<&str> {
        let host = self.host_str()?;
        let suffix = std::str::from_utf8(LIST.domain(host.as_bytes())?.as_bytes()).ok()?;
        let suffix = suffix.strip_prefix(".")?;
        let subdomain = host.strip_suffix(suffix)?;
        Some(subdomain)
    }
}
