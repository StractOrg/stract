import { Footer } from "../components/Footer.tsx";
import { Header } from "../components/Header.tsx";
import { Chat } from "../islands/Chat.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default function ChatRoute() {
  const withAlice = false;

  return (
    <>
      <div class="relative grid h-full grid-rows-[auto_1fr_auto]">
        <div id="header" class="row-start-1">
          <Header
            withAlice={withAlice}
            showDivider={true}
            active="Chat"
          />
        </div>
        <div class="flex flex-col row-start-2 mt-10 px-2 items-center">
          <Chat />
        </div>
        <div id="footer" class="row-start-3">
          <Footer />
        </div>
      </div>
    </>
  );
}
