#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        unsafe {
            let params = ggml_init_params {
                mem_size: 128 * 1024 * 1024,
                mem_buffer: std::ptr::null_mut(),
                no_alloc: false,
            };

            let ctx = ggml_init(params);
            let a = ggml_new_f32(ctx, 2.0);
            let b = ggml_new_f32(ctx, 2.0);

            let c = ggml_add(ctx, a, b);

            let gf = ggml_new_graph(ctx);
            ggml_build_forward_expand(gf, c);

            ggml_graph_compute_with_ctx(ctx, gf, 1);

            let res = ggml_get_f32_1d(c, 0);
            assert_eq!(res, 4.0);
        }
    }
}
