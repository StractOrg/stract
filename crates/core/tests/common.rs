use stract_core::index::Index;

pub type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

pub fn temporary_index() -> Result<Index> {
    let path = stdx::gen_temp_path();
    Index::open(path)
}

pub fn rand_words(num_words: usize) -> String {
    use rand::{distributions::Alphanumeric, Rng};
    let mut res = String::new();

    for _ in 0..num_words {
        res.push_str(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect::<String>()
                .as_str(),
        );
        res.push(' ');
    }

    res.trim().to_string()
}
