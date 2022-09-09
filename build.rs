use std::process::Command;

fn main() {
    Command::new("npm")
        .current_dir("frontend")
        .arg("install")
        .status()
        .unwrap();

    Command::new("npm")
        .current_dir("frontend")
        .args(&["run", "build"])
        .status()
        .unwrap();

    lalrpop::process_root().unwrap();
}
