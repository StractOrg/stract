from huggingface_hub import snapshot_download
import argparse

parser = argparse.ArgumentParser(description="Download a huggingface model to a local directory")

parser.add_argument(
    "local_dir", type=str
)

parser.add_argument(
    "repo_id", type=str
)

args = parser.parse_args()

snapshot_download(
    repo_id=args.repo_id,
    local_dir=args.local_dir,
    local_dir_use_symlinks=False,
    revision="main")
