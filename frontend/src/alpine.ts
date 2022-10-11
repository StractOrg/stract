import Alpine from "alpinejs";
// @ts-ignore
import persist from "@alpinejs/persist";

Alpine.plugin(persist);

// @ts-ignore
window.Alpine = Alpine;
Alpine.start();
