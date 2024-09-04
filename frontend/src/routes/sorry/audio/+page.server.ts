import type { PageServerLoadEvent } from './$types';
import { generateAudio } from '$lib/captcha/lib';
import { checkCaptcha } from '../common';

const extractChallenge = (searchParams: URLSearchParams): number[] => {
  const challenge = [];

  const challengeStr = searchParams.get('challenge') || '';
  for (let i = 0; i < challengeStr.length; i++) {
    challenge.push(Number(challengeStr[i]));
  }

  return challenge;
};

export const load = async ({ url, getClientAddress }: PageServerLoadEvent) => {
  const challenge = extractChallenge(url.searchParams);

  await checkCaptcha(url.searchParams, challenge, getClientAddress);

  const captcha = await generateAudio();

  return {
    audioBase64: captcha.audioBase64,
    resultDigestBase64: captcha.resultDigestBase64,
  };
};
