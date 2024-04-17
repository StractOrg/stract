# Background

Stract is an open source company that tries to fix search.
All our code is licensed under AGPLv3.
The main idea is that if we ever do anything that's against the best interest of the users, someone else can take our code and do better. You can read more about the [thought process here](https://github.com/StractOrg/stract/discussions/150#discussioncomment-8476851) if you are interested.

Please only contribute if you feel comfortable doing so.

# Contributing to Stract

There are many ways to contribute to Stract.
Code contribution are welcome of course, but also
bug reports, feature request, and evangelizing are just as valuable.

# Submitting a PR

Check if your issue is already listed [github](https://github.com/StractOrg/stract/issues).
If it is not, create your own issue. Please make sure that an issue exists before
submitting a PR. This will allow us to discuss the issue and make sure that the
PR is not a waste of your time.

Please add the following phrase at the end of your commit `Closes #<Issue Number>`.
It will automatically link your PR in the issue page. Also, once your PR is merged, it will
close the issue. If your PR only partially addresses the issue, and you would like to
keep it open, just write `See #<Issue Number>`.

Feel free to send your contribution in an unfinished state to get early feedback.
In that case, simply mark the PR with the tag [WIP] (standing for work in progress).

# Signing the CLA

Contributors are required to sign a Contributor License Agreement.
The process is simple and fast. Upon your first pull request, you will be prompted to
[sign our CLA by visiting this link](https://cla-assistant.io/StractOrg/stract).

We ask you to sign the CLA as it makes it [easier for us to sue a competitor](https://softwareengineering.stackexchange.com/questions/168020/how-signing-out-a-cla-prevents-legal-issues-in-open-source-projects/168026#168026)
if they take our code without making theirs open source, which would breach the AGPLv3 license.

# Development

## Setup

- Install rust by following the steps outlined [here](https://www.rust-lang.org/tools/install).
- Install clang and npm.
- Install liburing. If you're using a Debian based Linux, you can install it by running `sudo apt install liburing-dev`.
- Install [just](https://github.com/casey/just) by running `cargo install just`. This allows you to run the scripts in the `justfile` file. A justfile is basically a simple Makefile.
- Install [wasm-pack](https://rustwasm.github.io/wasm-pack/installer/) by running `cargo install wasm-pack`. This will allow for generation and packaging of client side WebAssembly code.
- Run the command `just configure` which should automatically configures the rest of your dev environment. The script creates a python virtual environment, installs relevant dependencies, traces and exports the ML models and creates a small local index which you can use for development.
- Install [cargo-watch](https://github.com/watchexec/cargo-watch) by running `cargo install cargo-watch`.
- (Optional) Install [abeye](https://github.com/oeb25/abeye) by running `cargo install --git https://github.com/oeb25/abeye --locked`. This is used for generating the API client used by the frontend by running `just openapi`.

After the non optional steps you can now run `cargo test` and should see all tests passing. If you have installced `cargo-watch`, you should be able to run `just dev` to start the search server and launch the frontend at `0.0.0.0:8000`.
