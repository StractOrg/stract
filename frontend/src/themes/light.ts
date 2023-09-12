import { base, theme } from './base';

export const lightTheme = theme({
  name: 'light',
  scheme: 'light',
  colors: {
    primary: base.navy[400],
    primaryFocus: base.navy[500],
    primaryContent: '#fff',

    secondary: base.island[400],
    secondaryFocus: base.island[500],
    secondaryContent: '#fff',

    accent: base.cocoa[400],
    accentFocus: base.cocoa[500],
    accentContent: '#fff',

    neutral: base.slate[600],
    neutralFocus: base.slate[700],
    neutralContent: base.island[100],

    base100: '#fff',
    base200: base.slate[100],
    base300: base.slate[200],
    baseContent: base.slate[900],

    info: base.navy[500],
    infoContent: base.navy[50],

    success: base.emerald[600],
    successContent: base.emerald[50],

    warning: base.amber[400],
    warningContent: base.amber[50],

    error: base.rose[600],
    errorContent: base.rose[50],

    link: base.sky[600],
    linkVisited: base.purple[500],
  },
});
