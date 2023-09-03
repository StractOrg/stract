import { match } from "ts-pattern";
import { ExecutionState } from "../../search/index.ts";

export type Event =
  | { type: "user"; message: string }
  | { type: "alice"; data: ExecutionState };

export type Source = {
  url: string;
  text: string;
};

export type Citation = {
  claim: string;
  source?: Source;
  nr: number;
  index: number;
};

export type Message = {
  from: "user" | "alice";
  active: boolean;
  body: (string | Citation)[];
  sources: (Source | undefined)[];
  queries: {
    query: string;
    active: boolean;
    result: {
      title: string;
      text: string;
      url: string;
      site: string;
    }[];
  }[];
};

export type ChatState = {
  messages: Message[];
  aliceIsTyping: boolean;
};
export const defaultChatState = (): ChatState => ({
  messages: [],
  aliceIsTyping: false,
});

const modifyLastAlice = (
  messages: Message[],
  f: (message: Message) => Message,
): Message[] => {
  const last = messages[messages.length - 1];
  if (last.from == "alice") {
    return [...messages.slice(0, messages.length - 1), f(last)];
  } else {
    return [
      ...messages,
      f({ from: "alice", body: [], active: true, queries: [], sources: [] }),
    ];
  }
};

export const chatReducer = (state: ChatState, event: Event): ChatState =>
  match(event).returnType<ChatState>().with(
    { type: "user" },
    ({ message }) => ({
      ...state,
      aliceIsTyping: true,
      messages: [...state.messages, {
        from: "user",
        active: false,
        body: [message],
        queries: [],
        sources: [],
      }],
    }),
  ).with(
    { type: "alice" },
    ({ data }) =>
      match(data).with(
        { type: "beginSearch" },
        ({ query }) => ({
          ...state,
          messages: modifyLastAlice(
            state.messages,
            (msg) => ({
              ...msg,
              queries: [...msg.queries, {
                query,
                active: true,
                result: [],
              }],
            }),
          ),
        }),
      ).with(
        { type: "searchResult" },
        ({ query, result }) => ({
          ...state,
          messages: modifyLastAlice(
            state.messages,
            (msg) => ({
              ...msg,
              queries: msg.queries.map((q) =>
                q.query == query
                  ? ({
                    ...q,
                    active: false,
                    result,
                  })
                  : q
              ),
            }),
          ),
        }),
      ).with(
        { type: "speaking" },
        ({ text }) => ({
          ...state,
          messages: modifyLastAlice(
            state.messages,
            (msg) => {
              const last = msg.body.at(-1);
              const nextBody = typeof last == "string"
                ? msg.body.slice(0, msg.body.length - 1)
                : msg.body;
              let remaining = typeof last == "string" ? last + text : text;
              const sources = [...msg.sources];

              while (true) {
                const matches = /\[query (\d+) source (\d+)\]/g.exec(remaining);
                if (!matches) break;

                const queryIdx = parseInt(matches[1]) - 1;
                const sourceIdx = parseInt(matches[2]) - 1;

                const query = msg.queries.at(queryIdx);
                const source = query?.result.at(sourceIdx);

                const before = remaining.slice(0, matches.index);
                const after = remaining.slice(
                  matches.index + matches[0].length,
                );
                nextBody.push(before);
                if (after.startsWith(".")) {
                  nextBody.push(".");
                  remaining = after.slice(1);
                } else {
                  remaining = after;
                }

                const claim = before.split(".").findLast(() => true)!;

                let nr = sources.findIndex((src) =>
                  src && src?.url == source?.url
                );
                if (nr == -1) {
                  nr = sources.length;
                  sources.push(source);
                }
                const citation: Citation = {
                  claim,
                  source,
                  nr,
                  index: nextBody.length,
                };
                nextBody.push(citation);
              }
              nextBody.push(remaining);

              return ({ ...msg, sources, body: nextBody });
            },
          ),
        }),
      ).with({ type: "done" }, (_) => ({
        ...state,
        messages: modifyLastAlice(
          state.messages,
          (msg) => ({ ...msg, active: false }),
        ),
        aliceIsTyping: false,
      }))
        .exhaustive(),
  ).exhaustive();
