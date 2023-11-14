import type { SearchResult } from "$lib/db";
import { saveSearchResults } from "$lib/db";

export async function POST({request}): Promise<Response> {
    const {qid, results}: {
        qid: string;
        results: SearchResult[];
    } = await request.json();

    saveSearchResults(qid, results);

    return new Response("OK");
}
  