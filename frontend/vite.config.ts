import { sites } from "@openai/sites-vite-plugin";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react(), sites()],
  build: { outDir: "dist/client", emptyOutDir: true },
  server: { port: 5173 },
  preview: { port: 4173 },
});
