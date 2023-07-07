#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

mod fusion;
mod string_agg;
mod tag;
mod synth;
mod sheet;
mod utils;

pub use {
    fusion::DataFusion
};

