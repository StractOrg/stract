import { base, theme } from './base';

export const uglyTheme = theme({
  name: 'ugly',
  scheme: 'light',
  colors: {
    primary: 'rgb(239, 153, 149)',
    primaryContent: 'rgb(40, 36, 37)',
    secondary: 'rgb(164, 203, 180)',
    secondaryContent: 'rgb(40, 36, 37)',
    accent: 'rgb(220, 136, 80)',
    accentContent: 'rgb(40, 36, 37)',
    neutral: 'rgb(46, 40, 42)',
    neutralContent: 'rgb(237, 230, 212)',
    base100: 'rgb(228, 216, 180)',
    base200: 'rgb(219, 202, 154)',
    base300: 'rgb(212, 191, 135)',
    baseContent: 'rgb(40, 36, 37)',
    info: 'rgb(37, 99, 235)',
    success: 'rgb(22, 163, 74)',
    warning: 'rgb(217, 119, 6)',
    error: 'rgb(220, 38, 38)',
    link: base.blue[700],
    linkVisited: base.indigo[600],
  },
});
