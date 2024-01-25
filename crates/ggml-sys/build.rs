use std::env;
use std::path::PathBuf;

fn generate_bindings() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindgen::builder()
        .header("ggml-src/include/ggml/ggml.h")
        .header("ggml-src/include/ggml/ggml-alloc.h")
        .header("ggml-src/include/ggml/ggml-backend.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();

    generate_bindings();

    let ggml_src = PathBuf::from("ggml-src")
        .join("src")
        .canonicalize()
        .unwrap();
    let mut build = cmake::Config::new(ggml_src);

    build.define("GGML_STATIC", "ON");

    if target_os == "linux" {
        build.define("GGML_OPENBLAS", "ON");
        println!("cargo:rustc-link-lib=openblas");
    }

    if target_os == "macos" {
        println!("cargo:rustc-link-lib=framework=Accelerate");
    }

    let dst = build.build();

    println!("cargo:rustc-link-search={}/lib", dst.display());

    println!("cargo:rerun-if-changed=ggml-src");
    println!("cargo:rustc-link-lib=static=ggml");
}
