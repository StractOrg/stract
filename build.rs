use std::process::Command;

fn main() {
    Command::new("npm")
        .args(&["run", "build"])
        .status()
        .unwrap();
    println!("cargo:rerun-if-changed=tailwind.css");
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=yarn.lock");
    println!("cargo:rerun-if-changed=tailwind.config.js");
    println!("cargo:rerun-if-changed=postcss.config.js");

    lalrpop::process_root().unwrap();
}
