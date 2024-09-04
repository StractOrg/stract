import type { PageServerLoadEvent } from './$types';
import { generateImage } from '$lib/captcha/lib';
import type { URLSearchParams } from 'url';
import { checkCaptcha } from './common';

const patchSelections = (searchParams: URLSearchParams): number[] => {
  const res = [];

  for (let i = 0; i < 9; i++) {
    const patch = searchParams.get(`patch[${i}]`);
    if (patch && patch === 'on') {
      res.push(i);
    }
  }

  return res;
};

export const load = async ({ url, getClientAddress, request }: PageServerLoadEvent) => {
  const challenge = patchSelections(url.searchParams);
  const clientAddress = request.headers.get('x-real-ip') || getClientAddress();
  await checkCaptcha(url.searchParams, challenge, clientAddress);

  const captcha = await generateImage();
  const imgBase64 = await captcha.image.getBase64Async('image/png');

  return {
    imgBase64,
    count: captcha.count,
    animal: captcha.animal,
    resultDigestBase64: captcha.resultDigestBase64,
    redirectTo: url.searchParams.get('redirectTo') || '/',
  };
};
