use std::io::Write;

use stract::alice::{
    ExecutionState, {AcceleratorConfig, Alice},
};

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // dont use this key in production
    let key = base64::decode("URyJQTjwUjTq6FSRoZGdbUdTIvqs/QxkPacQio8Lhxc=").unwrap();

    let model = Alice::open(
        "data/alice",
        "data/summarizer/dual_encoder",
        Some(AcceleratorConfig {
            layer_fraction: 1.0,
            device: tch::Device::Mps,
            kind: tch::Kind::Float,
        }),
        &key,
    )
    .unwrap();

    tracing::debug!("model loaded");

    let mut last_state = None;

    loop {
        println!();
        print!("> ");
        std::io::stdout().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        let gen = model
            .new_executor(
                input,
                last_state.clone(),
                "http://localhost:3000/beta/api/search".to_string(),
                None,
            )
            .unwrap();

        for n in gen {
            match n {
                ExecutionState::BeginSearch { query: _ } => {}
                ExecutionState::SearchResult {
                    query: _,
                    result: _,
                } => {}
                ExecutionState::Speaking { text } => {
                    print!("{}", text);
                    std::io::stdout().flush().unwrap();
                }
                ExecutionState::Done { state } => {
                    last_state = Some(state.decode().unwrap());
                }
            }
        }
    }
}
