use std::ptr::NonNull;
use std::sync::Arc;

use crate::context::InnerContext;
use crate::{Context, Dims, ValidDims};

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

pub struct Tensor<const DIMS: usize>
where
    Dims<DIMS>: ValidDims<DIMS>,
{
    ctx: Arc<InnerContext>,
    type_: GgmlType,
    ptr: NonNull<ggml_sys::ggml_tensor>,
    shape: [u64; DIMS],
}

impl Tensor<1> {
    pub fn copy_from_slice(&mut self, slice: &[f32]) {
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

    pub fn copy_to_slice(&self, out: &mut [f32]) {
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

impl<const DIMS: usize> Tensor<DIMS>
where
    Dims<DIMS>: ValidDims<DIMS>,
{
    pub fn new(ctx: &mut Context, t: GgmlType, shape: [u64; DIMS]) -> Self {
        let ptr = match DIMS {
            1 => unsafe {
                ggml_sys::ggml_new_tensor_1d(
                    ctx.inner_ctx().as_ptr(),
                    t.as_raw() as u32,
                    shape[0] as i64,
                )
            },
            2 => unsafe {
                ggml_sys::ggml_new_tensor_2d(
                    ctx.inner_ctx().as_ptr(),
                    t.as_raw() as u32,
                    shape[0] as i64,
                    shape[1] as i64,
                )
            },
            3 => unsafe {
                ggml_sys::ggml_new_tensor_3d(
                    ctx.inner_ctx().as_ptr(),
                    t.as_raw() as u32,
                    shape[0] as i64,
                    shape[1] as i64,
                    shape[2] as i64,
                )
            },
            4 => unsafe {
                ggml_sys::ggml_new_tensor_4d(
                    ctx.inner_ctx().as_ptr(),
                    t.as_raw() as u32,
                    shape[0] as i64,
                    shape[1] as i64,
                    shape[2] as i64,
                    shape[3] as i64,
                )
            },
            _ => unreachable!("{} is not a valid dimension", DIMS),
        };

        Self {
            ctx: Arc::clone(ctx.inner_ctx()),
            type_: t,
            shape,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub(crate) fn as_ptr(&self) -> *mut ggml_sys::ggml_tensor {
        self.ptr.as_ptr()
    }
}

macro_rules! std_ops_impl {
    ($trait:ident, $std_func:ident, $func:ident) => {
        impl<const DIMS: usize> std::ops::$trait for Tensor<DIMS>
        where
            Dims<DIMS>: ValidDims<DIMS>,
        {
            type Output = Tensor<DIMS>;
            fn $std_func(self, rhs: Self) -> Self::Output {
                if self.type_ != rhs.type_ {
                    panic!("tensor types do not match");
                }
                if self.shape != rhs.shape {
                    panic!("tensor shapes do not match");
                }

                let ptr = unsafe {
                    ggml_sys::$func(self.ctx.as_ptr(), self.ptr.as_ptr(), rhs.ptr.as_ptr())
                };

                Self {
                    ctx: Arc::clone(&self.ctx),
                    type_: self.type_,
                    shape: self.shape,
                    ptr: NonNull::new(ptr).unwrap(),
                }
            }
        }
    };
}

std_ops_impl!(Add, add, ggml_add);
std_ops_impl!(Sub, sub, ggml_sub);
std_ops_impl!(Mul, mul, ggml_mul);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Graph};
    use std::ops::*;

    macro_rules! test_1d {
        ($name:ident, $ops:ident, $a:expr, $b:expr, $res:expr) => {
            #[test]
            fn $name() {
                let mut ctx = Context::new(128 * 1024 * 1024, 1);

                let mut at = Tensor::new(&mut ctx, GgmlType::F32, [$a.len() as u64]);
                at.copy_from_slice(&$a);

                let mut bt = Tensor::new(&mut ctx, GgmlType::F32, [$b.len() as u64]);
                bt.copy_from_slice(&$b);

                let ct = at.$ops(bt);

                let mut graph = Graph::build(&ctx, ct);

                graph.compute(&ctx);

                let mut out = vec![0.0; $res.len()];
                graph.out().copy_to_slice(&mut out);

                assert_eq!(out, $res);
            }
        };
    }

    test_1d!(add_1d, add, [1.0, 2.0], [2.0, 2.0], [3.0, 4.0]);
    test_1d!(mul_1d, mul, [2.0, 5.0], [2.0, 3.0], [4.0, 15.0]);
    test_1d!(sub_1d, sub, [3.0, 10.0], [2.0, 4.0], [1.0, 6.0]);
}
