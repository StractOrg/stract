use tantivy::collector::TopDocs;

pub(crate) fn initial_collector() -> TopDocs {
    TopDocs::with_limit(20) // TODO: take harmonic centrality into account initially
}

#[cfg(test)]
mod tests {
    #[test]
    fn harmonic_ranking() {
        todo!();
    }
}
