import child_process from 'child_process';
import { v4 as uuidv4 } from 'uuid';
import fs from 'fs';

const exec = (args: string[]) => {
  const process = child_process.spawn('ffmpeg', args);
  process.stderr.setEncoding('utf-8');

  return new Promise((resolve, reject) => {
    process.on('error', () => {
      reject(new Error('FFmpeg is not available.'));
    });

    process.on('close', (code, signal) => {
      if (signal !== null) {
        reject(new Error(`FFmpeg was killed with signal ${signal}`));
      } else if (code !== 0) {
        reject(new Error(`FFmpeg exited with code ${code}`));
      } else {
        resolve(null);
      }
    });
  });
};

const complexArgs = (files: string[]): string => {
  let res = '';

  for (let i = 0; i < files.length; i++) {
    res += '[' + i + ':0]';
  }

  res += `concat=n=${files.length}:v=0:a=1`;

  return res;
};

export class AudioMixer {
  files: string[];

  constructor(files: string[]) {
    this.files = files;
  }

  async toBuffer() {
    const tempPath = `/tmp/${uuidv4()}.wav`;

    const args = ['-hide_banner', '-loglevel', 'error', '-y'];

    for (const file of this.files) {
      args.push('-i');
      args.push(file);
    }

    args.push('-filter_complex');
    args.push(complexArgs(this.files));

    args.push(tempPath);

    await exec(args);

    const res = fs.readFileSync(tempPath);
    fs.unlinkSync(tempPath);

    return res;
  }
}
