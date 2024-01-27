pub struct Dims<const DIMS: usize>
where
    Self: ValidDims<DIMS>, {}

pub trait ValidDims<const DIMS: usize> {}

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
