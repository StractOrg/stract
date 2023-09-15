import { base, theme } from './base';

export const darkTheme = theme({
  name: 'dark',
  scheme: 'dark',
  colors: {
    primary: base.navy[600],
    primaryFocus: base.navy[500],
    primaryContent: '#fff',

    secondary: base.island[500],
    secondaryFocus: base.island[400],
    secondaryContent: '#fff',

    accent: base.cocoa[500],
    accentFocus: base.cocoa[400],
    accentContent: '#fff',

    neutral: base.stone[400],
    neutralFocus: base.stone[300],
    neutralContent: base.stone[100],

    base100: base.stone[900],
    base200: base.stone[800],
    base300: base.stone[700],
    base400: base.stone[600],
    baseContent: base.island[50],

    info: base.navy[700],
    infoContent: base.navy[200],

    success: base.green[600],
    successContent: base.green[100],

    warning: base.amber[600],
    warningContent: base.amber[100],

    error: base.cerise2[600],
    errorContent: base.cerise2[100],

    link: base.sky[400],
    linkVisited: base.purple[500],
  },
});
