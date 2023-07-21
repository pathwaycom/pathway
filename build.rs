fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    pyo3_build_config::add_extension_module_link_args();
}
