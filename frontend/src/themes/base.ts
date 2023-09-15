import colors from 'tailwindcss/colors';
import plugin from 'tailwindcss/plugin';
import { colord } from 'colord';

type Color = string;

type Colors = {
  primary: Color;
  primaryFocus?: Color;
  primaryContent?: Color;

  secondary: Color;
  secondaryFocus?: Color;
  secondaryContent?: Color;

  accent: Color;
  accentFocus?: Color;
  accentContent?: Color;

  neutral: Color;
  neutralFocus?: Color;
  neutralContent?: Color;

  base100: Color;
  base200?: Color;
  base300?: Color;
  base400?: Color;
  baseContent?: Color;

  info: Color;
  infoFocus?: Color;
  infoContent?: Color;

  success: Color;
  successFocus?: Color;
  successContent?: Color;

  warning: Color;
  warningFocus?: Color;
  warningContent?: Color;

  error: Color;
  errorFocus?: Color;
  errorContent?: Color;

  link: Color;
  linkVisited: Color;
};

type ThemeOpts = {
  name: string;
  scheme: 'light' | 'dark';
  colors: Colors;
};

export const theme = (opts: ThemeOpts) => {
  // const foreground = (c: string) => (colord(c).isDark() ? '255 255 255' : '0 0 0');
  const foreground = (c: string) =>
    colord(c).isDark() ? colord(c).lighten(0.9).toHex() : colord(c).darken(0.9).toHex();
  const focus = (c: string) =>
    opts.scheme == 'light' ? colord(c).darken().toHex() : colord(c).lighten().toHex();

  const colors: Required<Colors> = {
    ...opts.colors,
    primaryFocus: opts.colors?.primaryFocus ?? focus(opts.colors.primary),
    primaryContent: opts.colors?.primaryContent ?? foreground(opts.colors.primary),
    secondaryFocus: opts.colors?.secondaryFocus ?? focus(opts.colors.secondary),
    secondaryContent: opts.colors?.secondaryContent ?? foreground(opts.colors.secondary),
    accentFocus: opts.colors?.accentFocus ?? focus(opts.colors.accent),
    accentContent: opts.colors?.accentContent ?? foreground(opts.colors.accent),
    neutralFocus: opts.colors?.neutralFocus ?? focus(opts.colors.neutral),
    neutralContent: opts.colors?.neutralContent ?? foreground(opts.colors.neutral),
    base200: opts.colors?.base200 ?? colord(opts.colors.base100).darken().toHex(),
    base300: opts.colors?.base300 ?? colord(opts.colors.base100).darken().darken().toHex(),
    base400: opts.colors?.base400 ?? colord(opts.colors.base100).darken().darken().toHex(),
    baseContent: opts.colors?.baseContent ?? foreground(opts.colors.base100),
    infoFocus: opts.colors?.infoFocus ?? focus(opts.colors.info),
    infoContent: opts.colors?.infoContent ?? foreground(opts.colors.info),
    successFocus: opts.colors?.successFocus ?? focus(opts.colors.success),
    successContent: opts.colors?.successContent ?? foreground(opts.colors.success),
    warningFocus: opts.colors?.warningFocus ?? focus(opts.colors.warning),
    warningContent: opts.colors?.warningContent ?? foreground(opts.colors.warning),
    errorFocus: opts.colors?.errorFocus ?? focus(opts.colors.error),
    errorContent: opts.colors?.errorContent ?? foreground(opts.colors.error),
  };

  return plugin(({ addComponents }) => {
    const toKebabCase = (s: string) => s.replaceAll(/([A-Z0-9]+)/g, (x) => `-${x.toLowerCase()}`);

    addComponents({
      [`.theme-${opts.name}`]: Object.fromEntries(
        Object.entries(colors).map(([shade, color]) => [`--${toKebabCase(shade)}`, hex2rgb(color)]),
      ),
    });
  });
};

export const hex2rgb = (hex: string) => {
  hex = colord(hex).toHex();
  const pick = (n: number) => parseInt(hex.slice(n, n + 2), 16);
  return hex.startsWith('#') ? `${pick(1)} ${pick(3)} ${pick(5)}` : hex;
};

export const base = {
  ...colors,
  // https://tailwind.ink?p=10.f4f9fadcf1fbb5dff785bfec539adc3f77cd345cb92a45961e2e6c121c45f7f9fbe4f0fdc7d8fb9fb5f37c8deb6568e4534bd74039b82c278a181857f9fafbefeffbded3f8c1adedad83e1955ed67c42c45d32a0402271241643fdfcfbfbeff1f7cbe4ee9ec7ec6ea5e14a89cc3168a6254c7a1a314b111afdfcfafbf0ebf8cfd7eea2b0ea7286dd4e64c635469f2833721c22461113fcfbf8faf0d8f5d7aee6ae7bd9814dc35e2ea6441d80331859231238160cfbfaf4f9f0bcf1dd80dbb94eba8f2a9b70157d560d60400c412c0b2b1b08faf9f3f7f0b9ede07bd2bd4aa69526847712695e0c51460b37300a251d08f1f6f3dbf0eaade6d072cca230ac722191491d7b351a5e2c1440220e271bf0f6f6d3f0f6a0e5eb68c9d12ea9b0218c8e1d73741a585a143c420d252e
  navy: {
    50: '#f4f9fa',
    100: '#dcf1fb',
    200: '#b5dff7',
    300: '#85bfec',
    400: '#539adc',
    500: '#3f77cd',
    600: '#345cb9',
    700: '#2a4596',
    800: '#1e2e6c',
    900: '#121c45',
  },
  indigo: {
    50: '#f9fafb',
    100: '#efeffb',
    200: '#ded3f8',
    300: '#c1aded',
    400: '#ad83e1',
    500: '#955ed6',
    600: '#7c42c4',
    700: '#5d32a0',
    800: '#402271',
    900: '#241643',
  },
  cerise: {
    50: '#fdfcfb',
    100: '#fbeff1',
    200: '#f7cbe4',
    300: '#ee9ec7',
    400: '#ec6ea5',
    500: '#e14a89',
    600: '#cc3168',
    700: '#a6254c',
    800: '#7a1a31',
    900: '#4b111a',
  },
  cerise2: {
    50: '#fdfcfa',
    100: '#fbf0eb',
    200: '#f8cfd7',
    300: '#eea2b0',
    400: '#ea7286',
    500: '#dd4e64',
    600: '#c63546',
    700: '#9f2833',
    800: '#721c22',
    900: '#461113',
  },
  cocoa: {
    50: '#fcfbf8',
    100: '#faf0d8',
    200: '#f5d7ae',
    300: '#e6ae7b',
    400: '#d9814d',
    500: '#c35e2e',
    600: '#a6441d',
    700: '#803318',
    800: '#592312',
    900: '#38160c',
  },
  gold: {
    50: '#fbfaf4',
    100: '#f9f0bc',
    200: '#f1dd80',
    300: '#dbb94e',
    400: '#ba8f2a',
    500: '#9b7015',
    600: '#7d560d',
    700: '#60400c',
    800: '#412c0b',
    900: '#2b1b08',
  },
  lemon: {
    50: '#faf9f3',
    100: '#f7f0b9',
    200: '#ede07b',
    300: '#d2bd4a',
    400: '#a69526',
    500: '#847712',
    600: '#695e0c',
    700: '#51460b',
    800: '#37300a',
    900: '#251d08',
  },
  green: {
    50: '#f1f6f3',
    100: '#dbf0ea',
    200: '#ade6d0',
    300: '#72cca2',
    400: '#30ac72',
    500: '#219149',
    600: '#1d7b35',
    700: '#1a5e2c',
    800: '#144022',
    900: '#0e271b',
  },
  island: {
    50: '#f0f6f6',
    100: '#d3f0f6',
    200: '#a0e5eb',
    300: '#68c9d1',
    400: '#2ea9b0',
    500: '#218c8e',
    600: '#1d7374',
    700: '#1a585a',
    800: '#143c42',
    900: '#0d252e',
  },
} as const;
