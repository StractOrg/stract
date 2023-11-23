import { base, theme } from './base';

export const darkTheme = theme({
  name: 'dark',
  scheme: 'dark',
  colors: {
    primary: base.navy[500],
    primaryFocus: base.navy[400],
    primaryContent: '#fff',

    secondary: base.island[500],
    secondaryFocus: base.island[400],
    secondaryContent: '#fff',

    accent: base.cocoa[500],
    accentFocus: base.cocoa[400],
    accentContent: '#fff',

    neutral: base.zinc[400],
    neutralFocus: base.zinc[100],
    neutralContent: base.zinc[300],

    base100: base.zinc[800],
    base200: base.zinc[700],
    base300: base.zinc[600],
    base400: base.zinc[500],
    baseContent: base.island[50],

    info: base.navy[600],
    infoContent: base.navy[100],

    success: base.green[600],
    successContent: base.green[100],

    warning: base.amber[600],
    warningContent: base.amber[100],

    error: base.cerise2[600],
    errorContent: base.cerise2[100],

    link: base.sky[500],
    linkVisited: base.purple[500],
  },
});
