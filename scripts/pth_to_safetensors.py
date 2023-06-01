import torch
from safetensors.torch import save_file, save_model
import argparse

parser = argparse.ArgumentParser(description="Convert a PyTorch model to SafeTensors.")
parser.add_argument("model", type=str, help="The model to convert.")
args = parser.parse_args()

args.model = args.model.replace(".pth", "")


def main():
    input = f"{args.model}.pth"
    output = f"{args.model}.safetensors"
    print(f"* Loading with Torch: {input}")
    weights = torch.load(input, map_location="cpu")

    # fix shared weights
    for name in weights:
        weights[name] = weights[name].clone().contiguous()

    print(f"* Saving with SafeTensors: {output}")
    save_file(weights, output)

    print("* Done.")


if __name__ == "__main__":
    main()
