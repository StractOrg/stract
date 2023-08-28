import { PrivacySelector } from "../../islands/PrivacySelector.tsx";
import { DEFAULT_ROUTE_CONFIG } from "../../search/utils.ts";

export const config = DEFAULT_ROUTE_CONFIG;

export default function Privacy() {
  return (
    <div class="flex flex-col space-y-3 [&_a]:underline [&_a]:font-medium">
      <h1 class="text-2xl font-medium">Privacy</h1>
      <div class="text-sm">
        By default, some data about your searches are stored on our servers. We
        primarily store the text you used for the search, and which results (if
        any) you clicked on. This data immensely helps us improve everyones
        search experience by training our ranking models.
      </div>

      <div class="text-sm">
        It is important to note, that <b class="font-extrabold">no</b>{" "}
        personal information is stored about your search. We don't store your
        IP, browser fingerprint or even a precise timestamp. In fact, you can
        see exactly what gets stored{" "}
        <a href="https://github.com/StractOrg/Stract/blob/main/core/src/improvement.rs#L37-#L51">
          here
        </a>.
      </div>

      <div class="text-sm">
        We hope you feel comfortable sharing this data with us as this makes the
        results much better for everyone, but we also respect if you don't. You
        can opt-out at any point by toggling the checkbox below and we wont
        store anything at all about your searches.
      </div>

      <PrivacySelector />
    </div>
  );
}
