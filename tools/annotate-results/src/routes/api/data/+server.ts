import type { RequestEvent } from "@sveltejs/kit";
import { getDataset } from "$lib/dataset";

export async function GET({}: RequestEvent): Promise<Response> {
  const data = getDataset();

  if (data) {
    return new Response(JSON.stringify({ data }));
  } else {
    return new Response("Failed to parse CSV data", { status: 500 });
  }
}
