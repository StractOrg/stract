import platform
import subprocess
import sys
import os
import requests
import zipfile
import shutil
from urllib.request import urlretrieve
import argparse


def download_file(url, filename):
    with open(filename, "wb") as f:
        response = requests.get(url, stream=True)
        total = response.headers.get("content-length")

        if total is None:
            f.write(response.content)
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(
                chunk_size=max(int(total / 1000), 1024 * 1024)
            ):
                downloaded += len(data)
                f.write(data)
                done = int(50 * downloaded / total)
                sys.stdout.write("\r[{}{}]".format("█" * done, "." * (50 - done)))
                sys.stdout.flush()
    sys.stdout.write("\n")


def install_libtorch(gpu=False):
    system = platform.system()

    if os.path.exists("./libtorch"):
        return

    if system == "Darwin":
        subprocess.check_call([sys.executable, "-m", "pip", "install", "torch"])
        site_packages = (
            subprocess.check_output(
                [sys.executable, "-c", "import site; print(site.getsitepackages()[0])"]
            )
            .decode()
            .strip()
        )
        torch_path = os.path.join(site_packages, "torch")
        shutil.copytree(torch_path, "./libtorch", dirs_exist_ok=True)
    else:
        if system == "Linux":
            if gpu:
                url = "https://download.pytorch.org/libtorch/cu117/libtorch-cxx11-abi-shared-with-deps-2.0.0%2Bcu117.zip"
            else:
                url = "https://download.pytorch.org/libtorch/cpu/libtorch-cxx11-abi-shared-with-deps-2.0.0%2Bcpu.zip"
        else:
            print("Unsupported system")
            return
        filename = "libtorch.zip"
        print("Downloading libtorch...")
        download_file(url, filename)
        print("Extracting libtorch...")
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extractall("./")
        print("Done!")
        os.remove(filename)


def create_symlinks():
    # Create symlinks from libtorch to core/libtorch
    if os.path.exists("./crates/core/libtorch"):
        return

    os.symlink("../libtorch", "./crates/core/libtorch")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and install libtorch.")
    parser.add_argument(
        "--gpu", action="store_true", help="Download libtorch with CUDA support."
    )
    args, _ = parser.parse_known_args()

    install_libtorch(args.gpu)
    create_symlinks()
