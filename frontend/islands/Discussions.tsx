import { match } from "ts-pattern";
import { HiChatBubbleLeftRightOutline } from "../icons/HiChatBubbleLeftRightOutline.tsx";
import { HiChevronDown } from "../icons/HiChevronDown.tsx";
import * as search from "../search/index.ts";
import { useState } from "preact/hooks";

export const Discussions = (
  { discussions }: { discussions: search.Webpage[] },
) => {
  const [showMore, setShowMore] = useState(false);

  const shownDiscussions = showMore ? discussions : discussions.slice(0, 4);

  return (
    <div class="flex flex-col space-y-1.5 overflow-hidden">
      <div class="flex space-x-1 text-lg">
        <HiChatBubbleLeftRightOutline class="w-4" />
        <span>Discussions</span>
      </div>
      <div class="flex flex-col">
        {shownDiscussions.map((discussion) => (
          <DiscussionItem discussion={discussion} />
        ))}
        <div
          class="mt-3 w-fit rounded-full border px-2 py-1 hover:cursor-pointer hover:bg-neutral-100"
          onClick={() => setShowMore((showMore) => !showMore)}
        >
          {showMore ? "Show less" : "Show more"}
        </div>
      </div>
    </div>
  );
};

const DiscussionItem = ({ discussion }: { discussion: search.Webpage }) => (
  <div class="overflow-hidden">
    <div>
      <a class="text-sm hover:no-underline" href={discussion.url}>
        {discussion.domain}
      </a>
    </div>
    <details class="group">
      <summary class="list-none flex cursor-pointer space-x-2">
        <a
          class="text-md truncate font-medium group-open:underline inline-block max-w-[calc(100%-10px)]"
          title={discussion.title}
          href={discussion.url}
        >
          {discussion.title}
        </a>
        <HiChevronDown class="w-4 transition group-open:rotate-180" />
      </summary>

      {match(discussion.snippet)
        .with(
          { type: "normal" },
          ({ date, text }) => (
            <div class="mb-3 text-sm font-normal text-snippet">
              {typeof date == "string" && (
                <span class="text-gray-500">{date}</span>
              )}
              <span
                class="[&:nth-child(2)]:before:content-['—']"
                dangerouslySetInnerHTML={{
                  __html: text,
                }}
              />
            </div>
          ),
        )
        .otherwise(() => null)}
    </details>
  </div>
);
