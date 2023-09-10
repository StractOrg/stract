import { DEFAULT_OPTICS } from "../../search/optics.ts";
import { OpticSelector } from "../OpticsSelector.tsx";
import { Signal, signal, useSignal } from "@preact/signals";
import { ComponentChildren } from "preact";
import { HiPaperAirplaneOutline } from "../../icons/HiPaperAirplaneOutline.tsx";
import { match, P } from "ts-pattern";
import * as search from "../../search/index.ts";
import { useEffect, useLayoutEffect, useRef } from "preact/hooks";
import ReactMarkdown from "https://esm.sh/react-markdown@8.0.7?alias=react:preact/compat,react-dom:preact/compat,@types/react:preact/compat&external=preact/compat";
import {
  chatReducer,
  Citation,
  defaultChatState,
  Event,
  Message,
  Source,
} from "./state.ts";
import { CHAT_TRACES } from "./traces.ts";
import { twMerge } from "tailwind-merge";

function useInterval(callback: () => void, delay: number | null) {
  const savedCallback = useRef(callback);

  // Remember the latest callback if it changes.
  useLayoutEffect(() => {
    savedCallback.current = callback;
  }, [callback]);

  // Set up the interval.
  useEffect(() => {
    // Don't schedule if no delay is specified.
    // Note: 0 is a valid value for delay.
    if (!delay && delay !== 0) {
      return;
    }

    const id = setInterval(() => savedCallback.current(), delay);

    return () => clearInterval(id);
  }, [delay]);
}

export const Chat = () => {
  const events = useSignal<Event[]>(CHAT_TRACES[0]);
  const markers = [
    0,
    ...events.value.map((e, idx) =>
      e.type == "user" ? idx : e.data.type == "speaking" ? false : idx
    ).filter((x): x is number => typeof x == "number")
      .map((idx) => idx + 1),
  ];
  const numEventsShown = useSignal(markers.length - 1);
  const realNumEventsShown = useSignal(0);

  useInterval(() => {
    const MIN_STEP = 2;
    const target = markers[numEventsShown.value];
    if (realNumEventsShown.value != target) {
      const delta = target - realNumEventsShown.value;
      realNumEventsShown.value += Math.abs(delta) < MIN_STEP
        ? delta
        : delta < 0
        ? Math.min(0.01 * delta, -MIN_STEP)
        : delta > 0
        ? Math.max(0.01 * delta, MIN_STEP)
        : 0;
    }
  }, 5);

  return (
    <>
      <div class="max-w-3xl w-full gap-y-5 flex flex-col h-full">
        {events.value.length > 0 &&
          (
            <details
              class={twMerge(`
                group transition-all border opacity-25 open:opacity-100 rounded-xl open:py-2 px-5 space-y-2
                border-transparent open:border-slate-50 dark:open:border-brand-900
                bg-white open:bg-brand-50 dark:bg-stone-800 dark:open:bg-stone-800
              `)}
            >
              <summary class="text-lg font-semibold cursor-pointer group-open:border-b group-open:pb-2 dark:border-stone-700">
                Debugging
              </summary>
              <details>
                <summary>Event trace</summary>
                <pre className="rounded p-4 bg-slate-white shadow select-all whitespace-pre-wrap">
                  {JSON.stringify(events.value)}
                </pre>
              </details>
              <div class="flex space-x-2">
                <input
                  class="flex-1"
                  type="range"
                  min={0}
                  max={markers.length - 1}
                  value={numEventsShown}
                  step={1}
                  onInput={(e) => {
                    numEventsShown.value = parseInt(
                      (e.target as HTMLInputElement).value,
                    );
                  }}
                />
                <button
                  class="py-1 px-2 rounded border"
                  onClick={() => events.value = []}
                >
                  Reset chat
                </button>
              </div>
              <div class="flex justify-between text-xs">
                {Array.from({ length: 30 }).map((_, idx, all) => {
                  const score = idx / (all.length - 1);
                  return (
                    <div
                      style={{
                        color: colorAtScore(score),
                      }}
                    >
                      [{(score * 100).toFixed(0)}]
                    </div>
                  );
                })}
              </div>
            </details>
          )}

        <ChatView
          events={events.value.slice(0, Math.round(realNumEventsShown.value))}
          onSend={(message) => {
            events.value = [...events.value, { type: "user", message }];
            const prevState = events.value.map((event) =>
              match(event).with({
                type: "alice",
                data: P.select({ type: "done" }),
              }, ({ state }) => state).otherwise(() => void 0)
            ).findLast((
              x,
            ): x is string => typeof x == "string" && x != "");

            search.api.alice({ message, prevState }, (event) =>
              match(event)
                .with({ type: "begin" }, () => {})
                .with({ type: "content" }, ({ data }) => {
                  events.value = [...events.value, {
                    type: "alice",
                    data,
                  }];
                })
                .with({ type: "done" }, () => {})
                .exhaustive());
          }}
        />
      </div>
    </>
  );
};

const ChatView = (
  { events, onSend }: { events: Event[]; onSend: (message: string) => void },
) => {
  const { messages, aliceIsTyping } = events.reduce(
    chatReducer,
    defaultChatState(),
  );

  return (
    <>
      <div
        id="message-container"
        class="flex px-5 flex-col gap-y-3 h-full leading-6"
      >
        {messages.length
          ? messages.map((msg) => <ChatMessage message={msg} />)
          : (
            <div class="flex flex-col h-full items-center gap-y-3">
              <h1 class="text-2xl font-bold">Chat with Alice</h1>
              <p class="text-sm">
                Alice is an AI that tries to answer your questions by searching
                for information on the internet. As always, you should verify
                the information from multiple sources and make sure the sources
                supports the claims. Alice will most likely be a paid feature in
                the future.
              </p>
              <div class="flex flex-col items-center pt-20 gap-y-3">
                <div class="w-20">
                  <img src="/images/warning.svg" />
                </div>
                <p class="text-contrast-400">
                  Alice is <b class="font-bold">highly experimental</b>{" "}
                  and might produce bad or downright wrong answers.
                </p>
              </div>
            </div>
          )}
      </div>
      <ChatInput disabled={aliceIsTyping} onSend={onSend} />
    </>
  );
};

const ChatMessage = (
  { message }: {
    message: Message;
  },
) => {
  const isUser = message.from == "user";

  const sources = message.body.filter((x): x is Citation =>
    typeof x != "string"
  ).map((c) => c);

  return (
    <div>
      <div
        class={twMerge("flex", isUser && "flex-row-reverse")}
      >
        {message.body.find((x) => x != "") && (
          <div
            class={twMerge(
              "p-2 rounded-xl border relative",
              isUser
                ? "bg-brand-50 border-brand-300 dark:bg-brand-950 dark:border-brand-800 rounded-br-none"
                : "bg-gray-50 border-gray-300 dark:bg-stone-800 dark:border-stone-700 rounded-bl-none",
            )}
          >
            <ReactMarkdown
              children={message.body.map((x) => (
                typeof x == "string" ? x : `\`cite:${JSON.stringify(x)}\``
              )).join("")}
              components={{
                code: (props: { children: ComponentChildren }) =>
                  match(props.children)
                    .with(
                      [P.string.startsWith("cite:").select()],
                      (source) => {
                        const citation: Citation = JSON.parse(
                          source.slice("cite:".length),
                        );
                        return <FactSource citation={citation} />;
                      },
                    ).otherwise(() => <code {...props} />),
              }}
            />
          </div>
        )}
        <div class="w-5 shrink-0" />
      </div>
      <div class="flex space-x-1 mt-2 cursor-default">
        {sources
          .filter(({ nr }, idx) =>
            sources.findIndex((other) => other.nr == nr) == idx
          )
          .map(({ source, nr }, idx) => (
            <FactReference
              source={source}
              correctness={void 0}
              href={source?.url}
              nr={nr}
              index={idx}
            />
          ))}
      </div>
      {message.active &&
        (
          <>
            <div class="my-2">
              <ChatBubble />
            </div>
            <div class="flex space-x-2">
              {message.queries.filter(({ active }) => active).map((
                { query },
              ) => <span class="italic">Searching for '{query}'...</span>)}
            </div>
          </>
        )}
    </div>
  );
};

const factCheckings: Signal<
  Record<
    string,
    void | Signal<"inprogress" | search.FactCheckResponse>
  >
> = signal({});

const FactSource = (
  { citation: { claim, source, nr, index } }: { citation: Citation },
) => {
  const key = `${claim}~${source?.text}`;

  useEffect(() => {
    if (!source) return;

    const prev = factCheckings.value[key];
    if (typeof prev == "undefined") {
      const s = signal<"inprogress" | search.FactCheckResponse>("inprogress");
      factCheckings.value = { ...factCheckings.value, [key]: s };
      const { data } = search.api.factCheck({
        claim,
        evidence: source.text,
      });
      data.then((res) => {
        s.value = res;
      });
    }
  }, [claim, source && source.url]);
  const factCheck = factCheckings.value[key];

  return (
    <FactReference
      source={source}
      claim={claim}
      correctness={typeof factCheck?.value == "object"
        ? factCheck.value.score
        : void 0}
      href={source && source.url}
      nr={nr}
      index={index}
    />
  );
};

const colorRed = { r: 220, g: 38, b: 38, a: 1 };
const colorYellow = { r: 234, g: 179, b: 8, a: 1 };
const colorGreen = { r: 22, g: 163, b: 74, a: 1 };
const gradient = [
  [colorRed, colorYellow],
  [colorYellow, colorGreen],
].flatMap(([a, b]) =>
  Array.from({ length: 5 }).map((_, step, steps) => {
    const t = step / steps.length;
    const interp = (k: "r" | "g" | "b" | "a") => a[k] + (b[k] - a[k]) * t;
    return { r: interp("r"), g: interp("g"), b: interp("b"), a: interp("a") };
  })
);
const colorAtScore = (score: number) => {
  const index = Math.min(
    gradient.length - 1,
    Math.floor(score * gradient.length),
  );
  const { r, g, b, a } = gradient[index];
  return `rgba(${r}, ${g}, ${b}, ${a})`;
};

const FactReference = (
  { source, claim, correctness, href, nr, index }: {
    source?: Source;
    claim?: string;
    correctness: void | number;
    href: string | void;
    nr: number;
    index: number;
  },
) => {
  const color = typeof correctness == "number"
    ? colorAtScore(correctness)
    : void 0;
  return (
    <span
      class="group inline-flex justify-center self-baseline -translate-y-1 relative"
      style={{ zIndex: 100 - index }}
    >
      <a
        href={href ?? void 0}
        target="__blank"
        class={twMerge(
          "text-xs",
          source
            ? "font-semibold"
            : "text-slate-400 dark:text-stone-500 animate-pulse",
        )}
        style={{ color }}
      >
        [{nr + 1}]
      </a>
      <div
        class={twMerge(`
        absolute flex flex-col text-sm group-hover:opacity-100 opacity-0 transition -left-2 -bottom-1 translate-y-full p-2 rounded z-10 shadow pointer-events-none space-y-2
        bg-white dark:bg-stone-900 dark:border dark:border-stone-700
      `)}
      >
        {source
          ? (
            <>
              <p class="line-clamp-1 text-xs text-slate-500 dark:text-brand-100">
                {source.url}
              </p>
              <p class="line-clamp-6 prose dark:prose-invert prose-sm w-[45ch] border-l pl-2">
                {source.text}
              </p>
              {typeof correctness == "number"
                ? (
                  <span class="flex space-x-1 font-normal">
                    <span class="">Fact check:</span>
                    <span class="place-self-end" style={{ color }}>
                      {(correctness * 100).toFixed(2)}% confidence
                    </span>
                  </span>
                )
                : claim
                ? "Fact checking..."
                : null}
            </>
          )
          : <span class="italic whitespace-nowrap">Hallucinated source</span>}
      </div>
    </span>
  );
};

const ChatInput = (
  { disabled, onSend }: {
    disabled: boolean;
    onSend: (message: string) => void;
  },
) => {
  const currentInput = useSignal("");

  return (
    <form
      class="w-full gap-y-2"
      disabled={disabled}
      onSubmit={(e) => {
        e.preventDefault();
        if (disabled) return;
        onSend(currentInput.value);
        currentInput.value = "";
      }}
    >
      <div class="flex w-full border rounded-xl p-2 pl-3 dark:border-slate-700">
        <div class="relative grow">
          <p class="text-transparent pointer-events-none whitespace-pre">
            {currentInput}.
          </p>
          <div class="absolute inset-0">
            <textarea
              autofocus
              class="w-full h-full resize-none outline-none focus:ring-0 max-h-52 border-none p-0 bg-transparent"
              placeholder="Type a message..."
              value={currentInput}
              onKeyDown={(e) => {
                match(e.key)
                  .with("Enter", () => {
                    if (e.shiftKey || currentInput.value == "") return;
                    e.preventDefault();
                    (e.target as HTMLTextAreaElement).form?.requestSubmit();
                  })
                  .otherwise(() => {});
              }}
              onInput={(e) => {
                currentInput.value = (e.target as HTMLTextAreaElement).value;
              }}
            />
          </div>
        </div>
        <div class="flex place-items-end">
          <button
            id="send-button"
            class="w-6 h-6 text-contrast-300 hover:text-contrast-500 cursor-pointer !bg-transparent"
            disabled={disabled}
          >
            <HiPaperAirplaneOutline />
          </button>
        </div>
      </div>
      <div class="w-full pt-2 text-gray-600 flex justify-end text-sm">
        <OpticSelector
          defaultOptics={DEFAULT_OPTICS}
          searchOnChange={false}
        />
      </div>
    </form>
  );
};

const ChatBubble = () => {
  return (
    <div class="flex items-center space-x-1">
      {Array.from({ length: 3 }).map((_, idx) => (
        <div
          class={"dot h-2 w-2 bg-brand-500 rounded-full animate-typing"}
          style={{
            animationDelay: `${200 + idx * 100}ms`,
          }}
        />
      ))}
    </div>
  );
};
