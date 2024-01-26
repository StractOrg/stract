use std::{ptr::NonNull, sync::Arc};

struct InnerContext {
    ctx: NonNull<ggml_sys::ggml_context>,
}

impl InnerContext {
    fn new(mem_size: usize) -> Self {
        let params = ggml_sys::ggml_init_params {
            mem_size,
            mem_buffer: std::ptr::null_mut(),
            no_alloc: false,
        };

        let ctx = unsafe { ggml_sys::ggml_init(params) };

        Self {
            ctx: NonNull::new(ctx).unwrap(),
        }
    }
    fn as_ptr(&self) -> *mut ggml_sys::ggml_context {
        self.ctx.as_ptr()
    }
}

impl Drop for InnerContext {
    fn drop(&mut self) {
        unsafe { ggml_sys::ggml_free(self.ctx.as_ptr()) }
    }
}

struct InnerGraph {
    ptr: NonNull<ggml_sys::ggml_cgraph>,
}

impl InnerGraph {
    fn new(ctx: &InnerContext) -> Self {
        let ptr = unsafe { ggml_sys::ggml_new_graph(ctx.as_ptr()) };

        Self {
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    fn as_ptr(&self) -> *mut ggml_sys::ggml_cgraph {
        self.ptr.as_ptr()
    }
}

pub struct Graph<const OUT_DIMS: usize> {
    inner: InnerGraph,
    out: Tensor<OUT_DIMS>,
}

impl<const OUT_DIMS: usize> Graph<OUT_DIMS> {
    fn inner(&self) -> &InnerGraph {
        &self.inner
    }

    pub fn build(ctx: &Context, out: Tensor<OUT_DIMS>) -> Self {
        let inner = InnerGraph::new(ctx.inner_ctx());

        unsafe {
            ggml_sys::ggml_build_forward_expand(inner.as_ptr(), out.as_ptr());
        }

        Self { inner, out }
    }

    pub fn compute(&mut self, ctx: &Context) {
        ctx.compute(self);
    }

    pub fn out(&mut self) -> &Tensor<OUT_DIMS> {
        &self.out
    }
}

pub struct Context {
    ctx: Arc<InnerContext>,
    n_threads: usize,
}

impl Context {
    pub fn new(mem_size: usize, n_threads: usize) -> Self {
        let ctx = Arc::new(InnerContext::new(mem_size));

        Self { ctx, n_threads }
    }

    pub fn build_graph<const OUT_DIMS: usize>(&self, out: Tensor<OUT_DIMS>) -> Graph<OUT_DIMS> {
        Graph::build(self, out)
    }

    fn inner_ctx(&self) -> &Arc<InnerContext> {
        &self.ctx
    }

    pub fn compute<const OUT_DIMS: usize>(&self, graph: &mut Graph<OUT_DIMS>) {
        unsafe {
            ggml_sys::ggml_graph_compute_with_ctx(
                self.ctx.as_ptr(),
                graph.inner().as_ptr(),
                self.n_threads as i32,
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum GgmlType {
    F32,
    F16,
    Q4_0,
    Q4_1,
    Q5_0,
    Q5_1,
    Q8_0,
    Q8_1,
    Q2_K,
    Q3_K,
    Q4_K,
    Q5_K,
    Q6_K,
    Q8_K,
    IQ2_XXS,
    IQ2_XS,
    I8,
    I16,
    I32,
    COUNT,
}

impl GgmlType {
    fn as_raw(&self) -> usize {
        match &self {
            GgmlType::F32 => 0,
            GgmlType::F16 => 1,
            GgmlType::Q4_0 => 2,
            GgmlType::Q4_1 => 3,
            GgmlType::Q5_0 => todo!(),
            GgmlType::Q5_1 => todo!(),
            GgmlType::Q8_0 => todo!(),
            GgmlType::Q8_1 => todo!(),
            GgmlType::Q2_K => todo!(),
            GgmlType::Q3_K => todo!(),
            GgmlType::Q4_K => todo!(),
            GgmlType::Q5_K => todo!(),
            GgmlType::Q6_K => todo!(),
            GgmlType::Q8_K => todo!(),
            GgmlType::IQ2_XXS => todo!(),
            GgmlType::IQ2_XS => todo!(),
            GgmlType::I8 => todo!(),
            GgmlType::I16 => todo!(),
            GgmlType::I32 => todo!(),
            GgmlType::COUNT => todo!(),
        }
    }
}

pub struct Tensor<const DIMS: usize> {
    ctx: Arc<InnerContext>,
    type_: GgmlType,
    ptr: NonNull<ggml_sys::ggml_tensor>,
    shape: [u64; DIMS],
}

impl Tensor<1> {
    pub fn new(ctx: &mut Context, t: GgmlType, shape: [u64; 1]) -> Self {
        let ptr = unsafe {
            ggml_sys::ggml_new_tensor_1d(
                ctx.inner_ctx().as_ptr(),
                t.as_raw() as u32,
                shape[0] as i64,
            )
        };

        Self {
            ctx: Arc::clone(ctx.inner_ctx()),
            type_: t,
            shape,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    fn copy_from_slice(&mut self, slice: &[f32]) {
        if slice.len() != self.shape[0] as usize {
            panic!("slice length does not match tensor shape");
        }

        if self.type_ != GgmlType::F32 {
            panic!("tensor type does not match slice type");
        }

        unsafe {
            let ptr = self.ptr.as_ptr().as_ref().unwrap().data;
            (ptr as *mut f32).copy_from_nonoverlapping(slice.as_ptr(), slice.len());
        }
    }

    fn copy_to_slice(&self, out: &mut [f32]) {
        if out.len() != self.shape[0] as usize {
            panic!("slice length does not match tensor shape");
        }

        if self.type_ != GgmlType::F32 {
            panic!("tensor type does not match slice type");
        }

        let data = unsafe {
            std::slice::from_raw_parts(
                self.ptr.as_ptr().as_ref().unwrap().data as *const f32,
                self.shape[0] as usize,
            )
        };

        out.copy_from_slice(data);
    }
}

// impl Tensor<2> {
//     pub fn new(ctx: &mut Context, t: GgmlType, shape: [u64; 2]) -> Self {
//         let ptr = unsafe {
//             ggml_sys::ggml_new_tensor_2d(
//                 ctx.inner_ctx().as_ptr(),
//                 t.as_raw() as u32,
//                 shape[0] as i64,
//                 shape[1] as i64,
//             )
//         };
//
//         Self {
//             ctx: Arc::clone(ctx.inner_ctx()),
//             type_: t,
//             shape,
//             ptr: NonNull::new(ptr).unwrap(),
//         }
//     }
// }
//
// impl Tensor<3> {
//     pub fn new(ctx: &mut Context, t: GgmlType, shape: [u64; 3]) -> Self {
//         let ptr = unsafe {
//             ggml_sys::ggml_new_tensor_3d(
//                 ctx.inner_ctx().as_ptr(),
//                 t.as_raw() as u32,
//                 shape[0] as i64,
//                 shape[1] as i64,
//                 shape[2] as i64,
//             )
//         };
//
//         Self {
//             ctx: Arc::clone(ctx.inner_ctx()),
//             type_: t,
//             shape,
//             ptr: NonNull::new(ptr).unwrap(),
//         }
//     }
// }
//
// impl Tensor<4> {
//     pub fn new(ctx: &mut Context, t: GgmlType, shape: [u64; 4]) -> Self {
//         let ptr = unsafe {
//             ggml_sys::ggml_new_tensor_4d(
//                 ctx.inner_ctx().as_ptr(),
//                 t.as_raw() as u32,
//                 shape[0] as i64,
//                 shape[1] as i64,
//                 shape[2] as i64,
//                 shape[3] as i64,
//             )
//         };
//
//         Self {
//             ctx: Arc::clone(ctx.inner_ctx()),
//             type_: t,
//             shape,
//             ptr: NonNull::new(ptr).unwrap(),
//         }
//     }
// }

impl<const DIMS: usize> Tensor<DIMS> {
    fn as_ptr(&self) -> *mut ggml_sys::ggml_tensor {
        self.ptr.as_ptr()
    }
}

impl<const DIMS: usize> std::ops::Add<Tensor<DIMS>> for Tensor<DIMS> {
    type Output = Tensor<DIMS>;

    fn add(self, rhs: Tensor<DIMS>) -> Self::Output {
        if self.type_ != rhs.type_ {
            panic!("tensor types do not match");
        }

        let ptr =
            unsafe { ggml_sys::ggml_add(self.ctx.as_ptr(), self.ptr.as_ptr(), rhs.ptr.as_ptr()) };

        Self {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            shape: self.shape,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }
}

// impl<const DIMS: usize> Tensor<DIMS> {
//     pub fn concat(&self, other: &Tensor<DIMS>) -> Tensor<DIMS+1> {
//         let ptr = unsafe {
//             ggml_sys::ggml_concat(self.ctx.as_ptr(), self.ptr.as_ptr(), other.ptr.as_ptr())
//         };
//
//         Self {
//             ctx: Arc::clone(&self.ctx),
//             type_: self.type_,
//             shape: self.shape,
//             ptr: NonNull::new(ptr).unwrap(),
//         }
//     }
// }
pub struct Dims<const DIMS: usize>
where
    Self: ValidDims<DIMS>, {}

trait ValidDims<const DIMS: usize> {}

macro_rules! valid_dims_impl {
    ($($dims:literal)*) => {
        $(
            impl ValidDims<$dims> for Dims<$dims> {}
        )*
    };
}

valid_dims_impl!(1 2 3 4);

trait DimsGt<const LHS: usize> {}

macro_rules! dims_gt_impl {
    ($($lhs:literal, $rhs:literal)*) => {
        $(
            impl DimsGt<$lhs> for Dims<$rhs>
            where
                Dims<$rhs>: ValidDims<$rhs>,
            {}
        )*
    };
}

dims_gt_impl!(1, 2);
dims_gt_impl!(1, 3);
dims_gt_impl!(1, 4);
dims_gt_impl!(2, 2);
dims_gt_impl!(2, 3);
dims_gt_impl!(2, 4);
dims_gt_impl!(3, 4);

trait DimsLt<const LHS: usize> {}

macro_rules! dims_lt_impl {
    ($($lhs:literal, $rhs:literal)*) => {
        $(
            impl DimsLt<$lhs> for Dims<$rhs>
            where
                Dims<$rhs>: ValidDims<$rhs>,
            {}
        )*
    };
}

dims_lt_impl!(2, 1);
dims_lt_impl!(3, 1);
dims_lt_impl!(4, 1);
dims_lt_impl!(2, 2);
dims_lt_impl!(3, 2);
dims_lt_impl!(4, 2);
dims_lt_impl!(3, 3);
dims_lt_impl!(4, 3);
dims_lt_impl!(4, 4);

trait DimsEq<const LHS: usize> {}

impl<const DIMS: usize> DimsEq<DIMS> for Dims<DIMS> where Dims<DIMS>: ValidDims<DIMS> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add() {
        let mut ctx = Context::new(128 * 1024 * 1024, 1);

        let mut a = Tensor::new(&mut ctx, GgmlType::F32, [1]);
        a.copy_from_slice(&[2.0]);

        let mut test = [0.0];
        a.copy_to_slice(&mut test);

        let mut b = Tensor::new(&mut ctx, GgmlType::F32, [1]);
        b.copy_from_slice(&[2.0]);

        let c = a + b;

        let mut graph = Graph::build(&ctx, c);

        graph.compute(&ctx);

        let mut out = [0.0];
        graph.out().copy_to_slice(&mut out);

        assert_eq!(out, [4.0]);
    }
}
