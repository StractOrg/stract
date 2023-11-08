import Papa from "papaparse";
import { readFileSync } from "fs";
import path from "path";

type Data = string[];

let dataset: Data | undefined = undefined;

export function getDataset(): Data | undefined {
  if (dataset) {
    return dataset;
  }

  const csvFilePath = path.resolve("../data/queries_us.csv");
  const csvFile = readFileSync(csvFilePath, "utf-8");

  Papa.parse(csvFile, {
    header: false,
    delimiter: "\n",
    complete: (result) => {
      dataset = result.data.map((row) => {
        return (row as string[])[0];
      });
    },
  });

  dataset = dataset!.sort(() => Math.random() - 0.5);

  return dataset;
}
