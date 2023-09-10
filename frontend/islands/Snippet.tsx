import { match } from "ts-pattern";
import * as search from "../search/index.ts";
import { summarySignals } from "../search/summary.ts";
import { HiHandThumbUpOutline } from "../icons/HiHandThumbUpOutline.tsx";
import { HiCheck } from "../icons/HiCheck.tsx";

const SAMPLE_SUMMARY = {
  inProgress: true,
  data:
    "This is a list of not-null Enums that will be initialized in an Xamarin project. It's supposed to be portable between devices, but when working with a local database it sticks to the local machine. This is problematic on a service with a SQL database installed.",
};

export const Snippet = ({ item }: { item: search.Webpage }) => {
  const summary = summarySignals.value[item.url];

  return (
    <div class="snippet">
      {summary
        ? (
          <div class="p-4 rounded-lg bg-slate-100 dark:bg-stone-800">
            <div class="font-light flex justify-between mb-2">
              <span>Summary</span>
              <button
                class="rounded px-2 text-xs bg-slate-200 text-slate-700 dark:bg-stone-700 dark:text-stone-100"
                onClick={() => {
                  summarySignals.value = {
                    ...summarySignals.value,
                    [item.url]: void 0,
                  };
                }}
              >
                hide summary
              </button>
            </div>
            <p class="line-clamp-3">
              {!summary.data
                ? (
                  Array.from({ length: 3 }).map((_, i) => (
                    <span
                      style={{ animationDelay: `${-i}00ms` }}
                      class="inline-block animate-bounce"
                    >
                      .
                    </span>
                  ))
                )
                : (
                  <>
                    {summary.data}
                    {summary.inProgress && (
                      <span
                        class={"inline-block -translate-y-0.5 font-thin"}
                        style={{
                          animation: "blink 1s steps(2) infinite",
                        }}
                      >
                        |
                      </span>
                    )}
                  </>
                )}
            </p>
          </div>
        )
        : match(item.snippet).with(
          { type: "normal" },
          ({ text, date }) => (
            <div class="line-clamp-3">
              {date && <span class="text-gray-500">{date}</span>}{" "}
              <div class="inline">
                <span
                  id="snippet-text"
                  class="[&:nth-child(2)]:before:content-['—'] snippet-text"
                  dangerouslySetInnerHTML={{
                    __html: text,
                  }}
                />
              </div>
            </div>
          ),
        ).with(
          { type: "stackOverflowQA" },
          ({ answers, question }) => (
            <StackOverflowQA answers={answers} question={question} />
          ),
        ).exhaustive()}
    </div>
  );
};

const StackOverflowQA = (
  { answers, question }: {
    answers: search.StackOverflowAnswer[];
    question: search.StackOverflowQuestion;
  },
) => (
  <div>
    <div>
      <div class="max-h-16 line-clamp-3 overflow-hidden break-words">
        {question.body.map((part) =>
          match(part).with(
            { type: "code" },
            ({ value }) => <span>{value}</span>,
          ).with(
            { type: "text" },
            ({ value }) => <span>{value}</span>,
          ).exhaustive()
        )}
      </div>
    </div>
    <div class="flex space-x-2 text-sm mt-2">
      {answers.map((answer) => <StackOverflowAnswer answer={answer} />)}
    </div>
  </div>
);

const StackOverflowAnswer = (
  { answer }: { answer: search.StackOverflowAnswer },
) => (
  <a
    class="h-52 w-1/3 rounded-lg bg-slate-100 p-4 hover:cursor-pointer hover:bg-slate-600 hover:text-white"
    href={answer.url}
  >
    <div class="h-full w-full overflow-hidden">
      <div class="mb-2 flex grow justify-between font-light">
        <div>{answer.date}</div>
        <div class="flex space-x-2">
          <div class="flex items-center space-x-1">
            <span class="h-fit">{answer.upvotes}</span>
            <div class="h-fit">
              <HiHandThumbUpOutline class="w-3" />
            </div>
          </div>
          {answer.accepted &&
            (
              <div>
                <HiCheck class="w-5 text-green-500" />
              </div>
            )}
        </div>
      </div>
      <div class="line-clamp-6">
        {answer.body.map(
          (part) => (
            <div class="select-none">
              {match(part)
                .with(
                  { type: "text" },
                  ({ value: text }) => <span>{text}</span>,
                )
                .with(
                  { type: "code" },
                  ({ value: code }) => (
                    <pre class="select-none">
                        <code class="select-none rounded-lg bg-none" style={{ background: "none" }}>
                          {code}
                        </code>
                    </pre>
                  ),
                ).exhaustive()}
            </div>
          ),
        )}
      </div>
    </div>
  </a>
);
