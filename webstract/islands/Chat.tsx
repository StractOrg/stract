import {
  animation,
  injectGlobal,
  keyframes,
  tw,
  tx,
} from "https://esm.sh/@twind/core@1.1.3";
import { DEFAULT_OPTICS } from "../search/optics.ts";
import { OpticSelector } from "./OpticsSelector.tsx";
import { useSignal } from "@preact/signals";
import { IS_BROWSER } from "$fresh/runtime.ts";
import { ComponentChildren } from "preact";
import { ExecutionState } from "../search/index.ts";
import { HiPaperAirplaneOutline } from "../icons/HiPaperAirplaneOutline.tsx";
import { match } from "ts-pattern";
import * as search from "../search/index.ts";
import { useEffect } from "preact/hooks";

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
  const { r, g, b, a } = gradient[Math.floor(score * gradient.length)];
  return `rgba(${r}, ${g}, ${b}, ${a})`;
};

type Event =
  | { type: "user"; message: string }
  | { type: "alice"; data: ExecutionState };

export const Chat = () => {
  const events = useSignal<Event[]>([]);

  const useSampleChat = true;
  const messages = useSignal<typeof SAMPLE_CHAT>(
    useSampleChat ? SAMPLE_CHAT : [],
  );

  const sources: Record<string, number> = {};
  const sourceNr = (queryIdx: number, sourceIdx: number) => {
    sources[`${queryIdx}-${sourceIdx}`] ??= Object.keys(sources).length + 1;
    return sources[`${queryIdx}-${sourceIdx}`];
  };

  const aliceIsTyping = false;

  return (
    <>
      <div class="max-w-3xl w-full gap-y-5 flex flex-col h-full">
        <div
          id="message-container"
          class="flex px-5 flex-col gap-y-3 h-full leading-6"
        >
          <details>
            <summary>Event trace</summary>
            <pre className="rounded p-4 bg-slate-50 shadow select-all whitespace-pre-wrap">
            {JSON.stringify(events.value)}
            </pre>
          </details>
          {messages.value.length
            ? messages.value.map((msg) => {
              const isUser = msg.from == "user";

              const children: (ComponentChildren | string)[] = [];

              let remaining = msg.message;

              while (true) {
                const matches = /\[query (\d+) source (\d+)\]/g.exec(remaining);
                if (!matches) break;

                const queryIdx = parseInt(matches[1]) - 1;
                const sourceIdx = parseInt(matches[2]) - 1;

                const query = msg.queries[queryIdx];
                const source = query.results[sourceIdx];

                const before = remaining.slice(0, matches.index);
                const after = remaining.slice(
                  matches.index + matches[0].length,
                );
                children.push(before);
                children.push(
                  <FactSource
                    claim={before.split(".").findLast(() => true)!}
                    source={source}
                    nr={sourceNr(queryIdx, sourceIdx)}
                  />,
                );

                remaining = after;
              }
              children.push(remaining);

              return (
                <div
                  class={tx("flex", isUser && "flex-row-reverse")}
                >
                  <div
                    class={tx(
                      "p-2 rounded-xl border ",
                      isUser
                        ? "bg-brand/5 border-brand/30 rounded-br-none"
                        : "bg-black bg-opacity-[0.025] border-black border-opacity-[0.15] rounded-bl-none",
                    )}
                  >
                    {children}
                  </div>
                  <div class="w-5 shrink-0" />
                </div>
              );
            })
            : (
              <div class="flex flex-col h-full items-center gap-y-3">
                <h1 class="text-2xl font-bold">Chat with Alice</h1>
                <p class="text-sm">
                  Alice is an AI that tries to answer your questions by
                  searching for information on the internet. As always, you
                  should verify the information from multiple sources and make
                  sure the sources supports the claims. Alice will most likely
                  be a paid feature in the future.
                </p>
                <div class="flex flex-col items-center pt-20 gap-y-3">
                  <div class="w-20">
                    <img src="/images/warning.svg" />
                  </div>
                  <p class="text-brand_contrast/75">
                    Alice is <b class="font-bold">highly experimental</b>{" "}
                    and might produce bad or downright wrong answers.
                  </p>
                </div>
              </div>
            )}
          {aliceIsTyping && <ChatBubble />}
        </div>
        <ChatInput
          onSend={(message) => {
            events.value = [...events.value, { type: "user", message }];

            search.api.alice({ message }, (event) =>
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

type Source = {
  url: string;
  text: string;
};

const FactSource = (
  { claim, source, nr }: { claim: string; source: Source; nr: number },
) => {
  const factCheck = useSignal<search.FactCheckResponse | null>(null);

  useEffect(() => {
    const { cancel, data } = search.api.factCheck({
      claim,
      evidence: source.text,
    });
    data.then((res) => factCheck.value = res);
    return cancel;
  }, [claim, source.url]);

  return (
    <span
      class="inline-flex rounded-full bg-white border px-1 text-xs justify-center"
      style={{
        borderColor: factCheck.value
          ? colorAtScore(factCheck.value.score)
          : void 0,
      }}
    >
      <a href={source.url}>{nr}</a>
    </span>
  );
};

const ChatInput = ({ onSend }: { onSend: (message: string) => void }) => {
  const currentInput = useSignal("");

  return (
    <form
      class="w-full gap-y-2"
      onSubmit={(e) => {
        e.preventDefault();
        onSend(currentInput.value);
        currentInput.value = "";
      }}
    >
      <div class="flex w-full border rounded-xl p-2 pl-3">
        <div class="relative grow">
          <p class="text-transparent pointer-events-none whitespace-pre">
            {currentInput.value}.
          </p>
          <div class="absolute inset-0">
            <textarea
              autofocus
              class="w-full h-full resize-none outline-none focus:ring-0 max-h-52 border-none p-0"
              placeholder="Type a message..."
              value={currentInput.value}
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
            class="w-6 h-6 text-brand_contrast/50 hover:text-brand_contrast cursor-pointer !bg-transparent"
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

const ChatBubble = () => (
  <div class="flex items-center space-x-1">
    {Array.from({ length: 3 }).map((_, idx) => (
      <div
        class={"dot h-2 w-2 bg-brand/70 rounded-full"}
        style={{
          animation: "mercuryTypingAnimation 1.8s infinite ease-in-out",
          animationDelay: `${200 + idx * 100}ms`,
        }}
      />
    ))}
  </div>
);

if (!IS_BROWSER) {
  injectGlobal`
@keyframes mercuryTypingAnimation {
  0% {
    transform: translateY(0px);
    @apply bg-brand/70;
  }
  28% {
    transform: translateY(-70px);
    @apply bg-brand/40;
  }
  44% {
    transform: translateY(0px);
    @apply bg-brand/20;
  }
}
`;
}

const SAMPLE_CHAT = [
  {
    "from": "user",
    "message": "can i rent a car in santa cruz",
    "queries": [],
    "sourceColors": [],
    "element": null,
  },
  {
    "from": "alice",
    "message":
      " Yes, you can rent a car in Santa Cruz de la Sierra from $34 per day[query 1 source 1]. The car rental rates vary depending on the type of vehicle you choose, the duration of the rental, and any additional fees. You can also find cheap car rental deals in Santa Cruz de la Sierra from $24 - $35 per day[query 1 source 1]. Additionally, you can rent a car for a month for just $34[query 1 source 2]. It's important to note that driving in Santa Cruz de la Sierra can be challenging due to the country's challenging road conditions and drivers who may not follow traffic laws[query 1 source 3]. It's recommended that you hire a licensed driver and take extra precautions when driving in the city[query 1 source 3].",
    "queries": [
      {
        "query": "rent car santa cruz",
        "results": [
          {
            "title":
              "Cheap Car Rental Deals in Santa Cruz de la Sierra from $34 - Cheapflights.com",
            "text":
              "month, costing you anywhere between $24 and $35 to fill up a car’s gas tank. Can I rent a car for a month in Santa Cruz de la Sierra? Yes,",
            "url": "https://www.cheapflights.com/car-rentals/santa-cruz/",
            "site": "cheapflights.com",
          },
          {
            "title":
              "What a Character Blogathon: How Arthur Kennedy Changed my Cinematic Life – The Wonderful World of Cinema",
            "text":
              "us smile, he’s just driving a car, but simply the fact that he’s here and that I was beginning to know him more and more. You know, just as if",
            "url":
              "https://thewonderfulworldofcinema.wordpress.com/2016/12/17/what-a-character-blogathon-how-arthur-kennedy-changed-my-cinematic-life/",
            "site": "thewonderfulworldofcinema.wordpress.com",
          },
          {
            "title":
              "Cop Killer Shouts Obscenities In Court, He Also Killed His Baby Mama… – Conversations Of A Sistah",
            "text":
              "likely. The devil didn’t make him steal his dads car, the devil didn’t make him kill those folks BUT now that he is up chit creek he is free to",
            "url":
              "https://conversationsofasistah.com/2012/01/31/cop-killer-shouts-obscenities-in-court-he-also-killed-his-baby-mama/",
            "site": "conversationsofasistah.com",
          },
        ],
      },
    ],
    "sourceColors": [],
    "element": null,
  },
];
