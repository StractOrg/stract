# Contributing to Stract
There are many ways to contribute to Stract.
Code contribution are welcome of course, but also
bug reports, feature request, and evangelizing are as valuable.

# Submitting a PR
Check if your issue is already listed [github](https://github.com/StractOrg/stract/issues).
If it is not, create your own issue. Please make sure that an issue exists before
submitting a PR. This will allow us to discuss the issue and make sure that the
PR is not a waste of your time.

Please add the following phrase at the end of your commit `Closes #<Issue Number>`.
It will automatically link your PR in the issue page. Also, once your PR is merged, it will
closes the issue. If your PR only partially addresses the issue and you would like to
keep it open, just write `See #<Issue Number>`.

Feel free to send your contribution in an unfinished state to get early feedback.
In that case, simply mark the PR with the tag [WIP] (standing for work in progress).

# Signing the CLA
Stract is an opensource project licensed a AGPLv3.

Contributors are required to sign a Contributor License Agreement.
The process is simple and fast. Upon your first pull request, you will be prompted to
[sign our CLA by visiting this link](https://cla-assistant.io/StractOrg/stract).

# Development
## Setup
* Install rust by following the steps outlined [here](https://www.rust-lang.org/tools/install)
* Install clang and npm
* Update ulimit. RocksDB tends to exceed the max number of allowed open files, so you will have to run `ulimit -n 10240` to increase the allowed max number of open files.
* Install [just](https://github.com/casey/just) by running `cargo install just`. This allows you to run the scripts in the `justfile` file. A justfile is basically a simple Makefile. Since libtorch requires some specific environment variables to be set, you need to preface all `cargo run` and `cargo test` commands with `just` (so they becom `just cargo run` and `just cargo test`) .
* Run the command `just configure` which should automatically configures the rest of your dev environment. The script creates a python virtual environment, installs relevant dependencies, traces and exports the ML models and creates a small local index which you can use for development.
* (Optional) Install [cargo-watch](https://github.com/watchexec/cargo-watch) by running `cargo install cargo-watch`. This makes frontend development easier.
* (Optional) Install [abeye](https://github.com/oeb25/abeye) by running `cargo install --git https://github.com/oeb25/abeye --locked`. This is used for generating the API client used by the frontend by running `just openapi`.

After the non optional steps you can now run `just cargo test` and should see all tests passing. If you have installced `cargo-watch`, you should be able to run `just dev` to start the search server and launch the frontend at `0.0.0.0:8000`.
