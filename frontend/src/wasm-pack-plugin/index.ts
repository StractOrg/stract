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
    watchChange(id, change) {
      console.log('watch change:', id, change);
    },
    buildStart() {
      for (const crate of config.crates) {
        const lib = path.resolve(normalizePath(crate));
        const job = childProcess.spawn('cargo-watch', ['-s', 'just'], {
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
