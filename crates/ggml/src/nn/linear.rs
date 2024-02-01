use crate::{Context, GgmlType, Tensor};

pub struct Linear {
    weight: Tensor<3>,
    bias: Option<Tensor<2>>,
}

impl Linear {
    pub fn new(ctx: &mut Context, input_size: u64, output_size: u64) -> Self {
        let weight = Tensor::new(ctx, GgmlType::default(), [input_size, output_size, 1]);

        Self { weight, bias: None }
    }

    pub fn forward(&self, input: &Tensor<3>) -> Tensor<3> {
        let res = &self.weight * input;

        if let Some(bias) = &self.bias {
            &res + &bias.repeat(&res)
        } else {
            res
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear() {
        let mut ctx = Context::new(128 * 1024 * 1024, 1);
        let input = Tensor::new(&mut ctx, GgmlType::default(), [128, 3, 5]);
        let linear = Linear::new(&mut ctx, 128, 256);
        let output = linear.forward(&input);
        assert_eq!(output.shape(), [256, 3, 5]);
    }
}
