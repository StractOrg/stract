import { normalizePath, type Plugin } from 'vite';
import childProcess from 'child_process';
import * as path from 'path';

type WasmPackConfig = {
  crates: string[];
};
export default function wasmPack(config: WasmPackConfig): Plugin {
  const jobs: (() => void)[] = [];

  return {
    name: 'wasm-pack',
    buildStart() {
      for (const crate of config.crates) {
        const lib = path.resolve(normalizePath(crate));
        const job = childProcess.spawn('cargo-watch', ['-s', 'wasm-pack build --target web'], {
          cwd: lib,
          env: { ...process.env, RUST_LOG: 'none' },
          stdio: 'inherit',
        });
        jobs.push(() => {
          job.kill();
        });
      }
    },
    buildEnd: () => {
      for (const job of jobs) {
        job();
      }
    },
  };
}
