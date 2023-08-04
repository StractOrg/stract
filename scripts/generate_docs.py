#!.venv/bin/python3
import os

path = "docs/dist"

if os.path.exists(path):
    os.system(f"rm -rf {path}")

os.mkdir(path)

os.system("cp docs/index.html docs/dist")
os.system("cp -r docs/img docs/dist/")

os.system("mdbook build docs/overview")
os.system("cp -r docs/overview/book docs/dist/overview")

os.system("just cargo doc")
os.system("mv target/doc/* docs/dist")
# os.system("cp -r target/doc/static.files docs/dist")
# os.system("cp -r target/doc/search-index.js docs/dist")
