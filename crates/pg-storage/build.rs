//! Build script: map the `loom` cargo feature onto `--cfg loom`.
//!
//! `cfg(loom)` is the conventional loom switch (the loom crate itself does
//! not set it). The `sync` module keys off it to swap parking_lot/std
//! primitives for loom's instrumented ones. Declaring the check-cfg keeps
//! `unexpected_cfgs` lint quiet on Rust 1.80+.

fn main() {
    println!("cargo::rustc-check-cfg=cfg(loom)");
    if std::env::var_os("CARGO_FEATURE_LOOM").is_some() {
        println!("cargo:rustc-cfg=loom");
    }
}
