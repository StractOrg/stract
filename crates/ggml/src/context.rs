use std::{ptr::NonNull, sync::Arc};

use crate::{Dims, Tensor, ValidDims};

pub(crate) struct InnerContext {
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

    pub(crate) fn as_ptr(&self) -> *mut ggml_sys::ggml_context {
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

pub struct Graph<const OUT_DIMS: usize>
where
    Dims<OUT_DIMS>: ValidDims,
{
    inner: InnerGraph,
    out: Tensor<OUT_DIMS>,
}

impl<const OUT_DIMS: usize> Graph<OUT_DIMS>
where
    Dims<OUT_DIMS>: ValidDims,
{
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

    pub fn build_graph<const OUT_DIMS: usize>(&self, out: Tensor<OUT_DIMS>) -> Graph<OUT_DIMS>
    where
        Dims<OUT_DIMS>: ValidDims,
    {
        Graph::build(self, out)
    }

    pub(crate) fn inner_ctx(&self) -> &Arc<InnerContext> {
        &self.ctx
    }

    pub fn compute<const OUT_DIMS: usize>(&self, graph: &mut Graph<OUT_DIMS>)
    where
        Dims<OUT_DIMS>: ValidDims,
    {
        unsafe {
            ggml_sys::ggml_graph_compute_with_ctx(
                self.ctx.as_ptr(),
                graph.inner().as_ptr(),
                self.n_threads as i32,
            );
        }
    }
}
