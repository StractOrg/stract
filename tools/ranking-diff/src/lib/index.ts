import type { SimpleWebpage } from './webpage';

export type Experiment = {
  id: number;
  name: string;
  timestamp: string;
};

export type Query = {
  id: number;
  text: string;
};

export type ExperimentResult = {
  experiment: Experiment;
  serp: SimpleWebpage[];
};
