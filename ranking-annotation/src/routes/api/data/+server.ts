import { getDataset } from "$lib/dataset";

export async function GET({}): Promise<Response> {
  const data = getDataset();

  if (data) {
    return new Response(JSON.stringify({ data }));
  } else {
    return new Response("Failed to parse CSV data", { status: 500 });
  }
}
