import Jimp from 'jimp';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { readdirSync } from 'fs';
import { AudioMixer } from './audioMixer';

/// A collection of custom captchas. They can very easily be broken
/// but hopefully stract is still small enough that very few will actually be
/// bothered to put in the effort.

const NUM_AUDIO_FILES = 5;
const IMG_PATCH_SIZE = 128;
const IMG_ROWS_COLS = 3;

const imgDir = `${dirname(fileURLToPath(import.meta.url))}/images`;
const audioDir = `${dirname(fileURLToPath(import.meta.url))}/audio`;

export type Animal = 'bunny' | 'cat' | 'dog' | 'duck' | 'hamster';
export const ANIMALS = ['bunny', 'cat', 'dog', 'duck', 'hamster'] satisfies Animal[];

export interface ImageCaptcha {
  image: Jimp;
  animal: Animal;
  count: number;
  resultDigestBase64: string;
}

const intoDigest = async (solution: number[]): Promise<string> => {
  const resultDigestBuffer = await crypto.subtle.digest('SHA-256', new Uint16Array(solution));
  return btoa(String.fromCharCode(...new Uint8Array(resultDigestBuffer)));
};

export const verifyDigest = async (digest: string, solution: number[]): Promise<boolean> => {
  const sol = await intoDigest(solution);

  return sol === digest;
};

export const generateImage = async (): Promise<ImageCaptcha> => {
  const image = await Jimp.create(IMG_PATCH_SIZE * IMG_ROWS_COLS, IMG_PATCH_SIZE * IMG_ROWS_COLS);

  let prevImages: Set<string> = new Set();
  const animals: Animal[] = [];
  const animalCounts = new Map();

  for (let i = 0; i < IMG_ROWS_COLS * IMG_ROWS_COLS; i++) {
    const x = Math.floor(i % IMG_ROWS_COLS) * IMG_PATCH_SIZE;
    const y = Math.floor(i / IMG_ROWS_COLS) * IMG_PATCH_SIZE;
    const animal = ANIMALS[Math.floor(Math.random() * ANIMALS.length)];
    animals.push(animal);
    const curCount = animalCounts.get(animal) | 0;
    animalCounts.set(animal, curCount + 1);

    const [animalImg, newPrevImages] = await readAnimalImage(animal, prevImages);
    prevImages = newPrevImages;

    image.composite(animalImg, x, y);
  }

  const maxAnimal = [...animalCounts.entries()].reduce((a, b) => (a[1] > b[1] ? a : b))[0];
  const result: number[] = [];

  for (let i = 0; i < animals.length; i++) {
    if (animals[i] == maxAnimal) {
      result.push(i);
    }
  }

  const resultDigestBase64 = await intoDigest(result);

  return { image, count: animalCounts.get(maxAnimal), animal: maxAnimal, resultDigestBase64 };
};

const readAnimalImage = async (
  animalType: Animal,
  prevImages: Set<string>,
): Promise<[Jimp, Set<string>]> => {
  try {
    const images = readdirSync(`${imgDir}/${animalType}`)
      .filter((img) => img != '.DS_Store')
      .filter((img) => !prevImages.has(`${imgDir}/${animalType}/${img}`));

    const imgName = images[Math.floor(Math.random() * images.length)];
    const imgPath = `${imgDir}/${animalType}/${imgName}`;

    return [await Jimp.read(imgPath), prevImages.add(imgPath)];
  } catch (err) {
    console.error('readAnimalImage', err);
    throw new Error(`Could not find animal image for ${animalType}`);
  }
};

export interface AudioCaptcha {
  audioBase64: string;
  resultDigestBase64: string;
}

export const generateAudio = async (): Promise<AudioCaptcha> => {
  const files = [];
  const solution = [];

  for (let i = 0; i < NUM_AUDIO_FILES; i++) {
    const number = Math.floor(Math.random() * 10);
    files.push(`${audioDir}/${number}.wav`);
    solution.push(number);
  }
  const resultDigestBase64 = await intoDigest(solution);

  const buffer = await new AudioMixer(files).toBuffer();

  const audioBase64 = `data:audio/wav;base64,${buffer.toString('base64')}`;

  return { audioBase64, resultDigestBase64 };
};
