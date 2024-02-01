use std::ptr::NonNull;
use std::sync::Arc;

use crate::context::InnerContext;
use crate::{Context, Dims, DimsGt, DimsPlusOne, ValidDims};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(non_camel_case_types)]
pub enum GgmlType {
    #[default]
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
    fn as_raw(&self) -> u32 {
        match &self {
            GgmlType::F32 => 0,
            GgmlType::F16 => 1,
            GgmlType::Q4_0 => 2,
            GgmlType::Q4_1 => 3,
            GgmlType::Q5_0 => 6,
            GgmlType::Q5_1 => 7,
            GgmlType::Q8_0 => 8,
            GgmlType::Q8_1 => 9,
            GgmlType::Q2_K => 10,
            GgmlType::Q3_K => 11,
            GgmlType::Q4_K => 12,
            GgmlType::Q5_K => 13,
            GgmlType::Q6_K => 14,
            GgmlType::Q8_K => 15,
            GgmlType::IQ2_XXS => 16,
            GgmlType::IQ2_XS => 17,
            GgmlType::I8 => 18,
            GgmlType::I16 => 19,
            GgmlType::I32 => 20,
            GgmlType::COUNT => 21,
        }
    }
}

pub struct Tensor<const DIMS: usize>
where
    Dims<DIMS>: ValidDims,
{
    ctx: Arc<InnerContext>,
    type_: GgmlType,
    ptr: NonNull<ggml_sys::ggml_tensor>,
}

impl<const DIMS: usize> Tensor<DIMS>
where
    Dims<DIMS>: ValidDims,
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
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub(crate) fn as_ptr(&self) -> *mut ggml_sys::ggml_tensor {
        self.ptr.as_ptr()
    }

    pub fn num_bytes(&self) -> usize {
        unsafe { ggml_sys::ggml_nbytes(self.ptr.as_ptr()) }
    }

    pub fn copy_from_slice(&mut self, slice: &[f32]) {
        if slice.len() as u64 != self.num_elements() {
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
        if out.len() as u64 != self.num_elements() {
            panic!("slice length does not match tensor shape");
        }

        if self.type_ != GgmlType::F32 {
            panic!("tensor type does not match slice type");
        }

        let data = unsafe {
            std::slice::from_raw_parts(
                self.ptr.as_ptr().as_ref().unwrap().data as *const f32,
                self.num_elements() as usize,
            )
        };

        out.copy_from_slice(data);
    }

    pub fn num_elements(&self) -> u64 {
        self.shape().into_iter().product()
    }

    pub fn shape(&self) -> [u64; DIMS] {
        let shape = unsafe { self.ptr.as_ptr().as_ref().unwrap().ne };

        let mut out = [0; DIMS];

        for i in 0..DIMS {
            out[i] = shape[i] as u64;
        }

        out
    }

    pub fn get_rows<const ROW_DIMS: usize, const OUT_DIMS: usize>(
        &self,
        rows: &Tensor<ROW_DIMS>,
    ) -> Tensor<OUT_DIMS>
    where
        Dims<ROW_DIMS>: ValidDims,
        Dims<OUT_DIMS>: DimsPlusOne<ROW_DIMS> + ValidDims,
    {
        debug_assert_eq!(rows.type_, GgmlType::I32, "rows tensor must be of type I32");

        let ptr = unsafe {
            ggml_sys::ggml_get_rows(self.ctx.as_ptr(), self.ptr.as_ptr(), rows.ptr.as_ptr())
        };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn repeat<const OUT_DIMS: usize>(&self, rep: &Tensor<OUT_DIMS>) -> Tensor<OUT_DIMS>
    where
        Dims<OUT_DIMS>: ValidDims,
        Dims<OUT_DIMS>: DimsGt<DIMS>,
    {
        let ptr =
            unsafe { ggml_sys::ggml_repeat(self.ctx.as_ptr(), self.ptr.as_ptr(), rep.as_ptr()) };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn reshape<const OUT_DIMS: usize>(&self, shape: [u64; OUT_DIMS]) -> Tensor<OUT_DIMS>
    where
        Dims<OUT_DIMS>: ValidDims,
    {
        debug_assert_eq!(
            shape.into_iter().product::<u64>(),
            self.num_elements(),
            "reshape must not change the number of elements"
        );

        let ptr = match OUT_DIMS {
            1 => unsafe {
                ggml_sys::ggml_reshape_1d(self.ctx.as_ptr(), self.ptr.as_ptr(), shape[0] as i64)
            },
            2 => unsafe {
                ggml_sys::ggml_reshape_2d(
                    self.ctx.as_ptr(),
                    self.ptr.as_ptr(),
                    shape[0] as i64,
                    shape[1] as i64,
                )
            },
            3 => unsafe {
                ggml_sys::ggml_reshape_3d(
                    self.ctx.as_ptr(),
                    self.ptr.as_ptr(),
                    shape[0] as i64,
                    shape[1] as i64,
                    shape[2] as i64,
                )
            },
            4 => unsafe {
                ggml_sys::ggml_reshape_4d(
                    self.ctx.as_ptr(),
                    self.ptr.as_ptr(),
                    shape[0] as i64,
                    shape[1] as i64,
                    shape[2] as i64,
                    shape[3] as i64,
                )
            },
            _ => unreachable!("{} is not a valid dimension", DIMS),
        };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn permute<const OUT_DIMS: usize>(&self, shape: [u64; OUT_DIMS]) -> Tensor<OUT_DIMS>
    where
        Dims<OUT_DIMS>: ValidDims,
    {
        let ptr = match OUT_DIMS {
            1 => unsafe {
                ggml_sys::ggml_permute(
                    self.ctx.as_ptr(),
                    self.ptr.as_ptr(),
                    shape[0] as i32,
                    1,
                    2,
                    3,
                )
            },
            2 => unsafe {
                ggml_sys::ggml_permute(
                    self.ctx.as_ptr(),
                    self.ptr.as_ptr(),
                    shape[0] as i32,
                    shape[1] as i32,
                    2,
                    3,
                )
            },
            3 => unsafe {
                ggml_sys::ggml_permute(
                    self.ctx.as_ptr(),
                    self.ptr.as_ptr(),
                    shape[0] as i32,
                    shape[1] as i32,
                    shape[2] as i32,
                    3,
                )
            },
            4 => unsafe {
                ggml_sys::ggml_permute(
                    self.ctx.as_ptr(),
                    self.ptr.as_ptr(),
                    shape[0] as i32,
                    shape[1] as i32,
                    shape[2] as i32,
                    shape[3] as i32,
                )
            },
            _ => unreachable!("{} is not a valid dimension", DIMS),
        };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn softmax(&self) -> Tensor<DIMS> {
        let ptr = unsafe { ggml_sys::ggml_soft_max(self.ctx.as_ptr(), self.ptr.as_ptr()) };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn scale(&self, scale: f32) -> Tensor<DIMS> {
        let ptr = unsafe { ggml_sys::ggml_scale(self.ctx.as_ptr(), self.ptr.as_ptr(), scale) };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn contiguous(&self) -> Tensor<DIMS> {
        let ptr = unsafe { ggml_sys::ggml_cont(self.ctx.as_ptr(), self.ptr.as_ptr()) };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn as_type(&self, type_: GgmlType) -> Tensor<DIMS> {
        let ptr =
            unsafe { ggml_sys::ggml_cast(self.ctx.as_ptr(), self.ptr.as_ptr(), type_.as_raw()) };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn norm(&self, eps: f32) -> Tensor<DIMS> {
        let ptr = unsafe { ggml_sys::ggml_norm(self.ctx.as_ptr(), self.ptr.as_ptr(), eps) };

        Tensor {
            ctx: Arc::clone(&self.ctx),
            type_: self.type_,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn load_bytes(&mut self, bytes: &[u8]) {
        debug_assert_eq!(
            bytes.len(),
            self.num_bytes(),
            "bytes must be the same size as the tensor"
        );

        unsafe {
            debug_assert_ne!(
                self.as_ptr().as_mut().unwrap().data,
                std::ptr::null_mut(),
                "tensor must be allocated"
            );

            (self.as_ptr().as_mut().unwrap().data as *mut u8)
                .copy_from_nonoverlapping(bytes.as_ptr(), self.num_bytes());
        }
    }
}

macro_rules! std_ops_impl {
    ($trait:ident, $std_func:ident, $func:ident) => {
        impl<const DIMS: usize> std::ops::$trait for Tensor<DIMS>
        where
            Dims<DIMS>: ValidDims,
        {
            type Output = Tensor<DIMS>;
            fn $std_func(self, rhs: Self) -> Self::Output {
                if self.type_ != rhs.type_ {
                    panic!("tensor types do not match");
                }

                let ptr = unsafe {
                    ggml_sys::$func(self.ctx.as_ptr(), self.ptr.as_ptr(), rhs.ptr.as_ptr())
                };

                Self::Output {
                    ctx: Arc::clone(&self.ctx),
                    type_: self.type_,
                    ptr: NonNull::new(ptr).unwrap(),
                }
            }
        }

        impl<'a, const DIMS: usize> std::ops::$trait<Tensor<DIMS>> for &'a Tensor<DIMS>
        where
            Dims<DIMS>: ValidDims,
        {
            type Output = Tensor<DIMS>;
            fn $std_func(self, rhs: Tensor<DIMS>) -> Self::Output {
                if self.type_ != rhs.type_ {
                    panic!("tensor types do not match");
                }

                let ptr = unsafe {
                    ggml_sys::$func(self.ctx.as_ptr(), self.ptr.as_ptr(), rhs.ptr.as_ptr())
                };

                Self::Output {
                    ctx: Arc::clone(&self.ctx),
                    type_: self.type_,
                    ptr: NonNull::new(ptr).unwrap(),
                }
            }
        }

        impl<'b, const DIMS: usize> std::ops::$trait<&'b Tensor<DIMS>> for Tensor<DIMS>
        where
            Dims<DIMS>: ValidDims,
        {
            type Output = Tensor<DIMS>;
            fn $std_func(self, rhs: &'b Tensor<DIMS>) -> Self::Output {
                if self.type_ != rhs.type_ {
                    panic!("tensor types do not match");
                }

                let ptr = unsafe {
                    ggml_sys::$func(self.ctx.as_ptr(), self.ptr.as_ptr(), rhs.ptr.as_ptr())
                };

                Self::Output {
                    ctx: Arc::clone(&self.ctx),
                    type_: self.type_,
                    ptr: NonNull::new(ptr).unwrap(),
                }
            }
        }

        impl<'a, 'b, const DIMS: usize> std::ops::$trait<&'b Tensor<DIMS>> for &'a Tensor<DIMS>
        where
            Dims<DIMS>: ValidDims,
        {
            type Output = Tensor<DIMS>;
            fn $std_func(self, rhs: &'b Tensor<DIMS>) -> Self::Output {
                if self.type_ != rhs.type_ {
                    panic!("tensor types do not match");
                }

                let ptr = unsafe {
                    ggml_sys::$func(self.ctx.as_ptr(), self.ptr.as_ptr(), rhs.ptr.as_ptr())
                };

                Self::Output {
                    ctx: Arc::clone(&self.ctx),
                    type_: self.type_,
                    ptr: NonNull::new(ptr).unwrap(),
                }
            }
        }
    };
}

std_ops_impl!(Add, add, ggml_add);
std_ops_impl!(Sub, sub, ggml_sub);
std_ops_impl!(Mul, mul, ggml_mul_mat);

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
    test_1d!(mul_1d, mul, [2.0, 5.0], [2.0, 3.0], [19.0]);
    test_1d!(sub_1d, sub, [3.0, 10.0], [2.0, 4.0], [1.0, 6.0]);

    #[test]
    fn num_bytes() {
        let mut ctx = Context::new(128 * 1024 * 1024, 1);
        let at = Tensor::new(&mut ctx, GgmlType::F32, [2, 3, 4]);
        assert_eq!(at.num_bytes(), 32 * 2 * 3 * 4 / 8);
    }

    #[test]
    fn load_bytes() {
        let mut ctx = Context::new(128 * 1024 * 1024, 1);
        let mut at = Tensor::new(&mut ctx, GgmlType::F32, [2]);
        at.load_bytes(&[1, 2, 3, 4, 5, 6, 7, 8]);
    }
}
