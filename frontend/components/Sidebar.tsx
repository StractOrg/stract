import { match, P } from "ts-pattern";
import * as search from "../search/index.ts";
import { Code } from "./Code.tsx";
import { HiHandThumbUp } from "../icons/HiHandThumbUp.tsx";

export const Sidebar = ({ sidebar }: { sidebar: search.Sidebar }) => (
  match(sidebar)
    .with({ type: "entity" }, ({ value }) => <Entity entity={value} />)
    .with(
      { type: "stackOverflow" },
      ({ value: { title, answer } }) => (
        <StackOverflow title={title} answer={answer} />
      ),
    ).exhaustive()
);

const Entity = ({ entity }: { entity: search.Entity }) => (
  <div class="flex w-full justify-center">
    <div class="flex w-full flex-col items-center">
      {entity.imageBase64 &&
        (
          <div class="w-lg mb-5">
            <a
              href={`https://en.wikipedia.org/wiki/${encodeURI(entity.title)}`}
            >
              <img
                class="h-full w-full rounded-full"
                src={`data:image/png;base64, ${entity.imageBase64}`}
              />
            </a>
          </div>
        )}
      <div class="mb-5 text-xl">
        <a
          class="hover:underline"
          href={`https://en.wikipedia.org/wiki/${encodeURI(entity.title)}`}
        >
          {entity.title}
        </a>
      </div>
      <div class="text-sm">
        <span dangerouslySetInnerHTML={{ __html: entity.smallAbstract }} />{" "}
        <span class="italic">
          source:{" "}
          <a
            class="hover:underline text-blue-600"
            href={`https://en.wikipedia.org/wiki/${encodeURI(entity.title)}`}
          >
            wikipedia
          </a>
        </span>
      </div>
      {entity.info.length > 0 &&
        (
          <div class="mt-7 mb-2 flex w-full flex-col px-4 text-sm">
            <div class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2">
              {entity.info.map(([key, value]) => (
                <>
                  <div
                    class="text-gray-500"
                    dangerouslySetInnerHTML={{ __html: key }}
                  />
                  <div dangerouslySetInnerHTML={{ __html: value }} />
                </>
              ))}
            </div>
          </div>
        )}
      {entity.relatedEntities.length > 0 &&
        (
          <div class="mt-5 flex w-full flex-col text-gray-500">
            <div class="font-light">Related Searches</div>
            <div class="flex overflow-scroll">
              {entity.relatedEntities.map((entity) => (
                <div class="flex flex-col items-center p-4">
                  {entity.imageBase64 &&
                    (
                      <div class="mb-3 h-20 w-20">
                        <a
                          href={`/search?q=${encodeURIComponent(entity.title)}`}
                        >
                          <img
                            class="h-full w-full rounded-full object-cover"
                            src={`data:image/png;base64, ${entity.imageBase64}`}
                          />
                        </a>
                      </div>
                    )}

                  <div class="text-center line-clamp-3">
                    <a
                      href={`/search?q=${encodeURI(entity.title)}`}
                    >
                      {entity.title}
                    </a>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
    </div>
  </div>
);
const StackOverflow = (
  { title, answer }: { title: string; answer: search.StackOverflowAnswer },
) => (
  <div class="flex md:max-w-lg flex-col space-y-5 rounded-lg border p-5 overflow-hidden">
    <div class="flex flex-col space-y-1">
      <div class="flex grow justify-between space-x-2">
        <div>
          <a class="text-lg font-medium leading-3" href={answer.url}>{title}</a>
        </div>
        <div class="flex items-center space-x-1">
          <span class="h-fit">
            {answer.upvotes}
          </span>
          <div class="h-fit">
            <HiHandThumbUp class="w-4" />
          </div>
        </div>
      </div>
      <div class="flex grow justify-between space-x-2">
        <div>
          <a href={answer.url}>{answer.url}</a>
        </div>
        <div>{answer.date}</div>
      </div>
    </div>
    <hr />
    <div class="flex flex-col space-y-3">
      {answer.body.map((part) => (
        <div class="">
          {match(part)
            .with({ "type": "text" }, (p) => <span>{p.value}</span>)
            .with(
              { "type": "code" },
              (p) => (
                <div class="rounded-lg bg-slate-50">
                  <div class="overflow-auto px-3 py-2">
                    <Code
                      lang="js"
                      code={p.value}
                    />
                  </div>
                </div>
              ),
            )
            .exhaustive()}
        </div>
      ))}
    </div>
  </div>
);
