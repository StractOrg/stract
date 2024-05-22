import type { SearchResult } from "$lib/db";
import { saveSearchResults } from "$lib/db";
import type { RequestEvent } from "@sveltejs/kit";

export async function POST({ request }: RequestEvent): Promise<Response> {
  const {
    qid,
    results,
  }: {
    qid: string;
    results: SearchResult[];
  } = await request.json();

  saveSearchResults(qid, results);

  return new Response("OK");
}
