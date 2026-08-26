import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { extname, join } from "node:path";

const clientDirectory = new URL("../dist/client/", import.meta.url);
const mimeTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".webp": "image/webp",
  ".woff2": "font/woff2",
};

async function collect(directory, prefix = "") {
  const assets = {};
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const relativePath = join(prefix, entry.name).replaceAll("\\\\", "/");
    const absolutePath = new URL(entry.name, directory);
    if (entry.isDirectory()) {
      Object.assign(assets, await collect(new URL(`${entry.name}/`, directory), relativePath));
      continue;
    }
    const content = await readFile(absolutePath);
    assets[`/${relativePath}`] = {
      body: content.toString("base64"),
      type: mimeTypes[extname(entry.name)] ?? "application/octet-stream",
    };
  }
  return assets;
}

const embeddedAssets = await collect(clientDirectory);
const output = `const assets=${JSON.stringify(embeddedAssets)};
function decode(value){const binary=atob(value);const bytes=new Uint8Array(binary.length);for(let i=0;i<binary.length;i+=1)bytes[i]=binary.charCodeAt(i);return bytes}
function assetResponse(asset,path){return new Response(decode(asset.body),{headers:{"Content-Type":asset.type,"Cache-Control":path==="/index.html"?"no-cache":"public, max-age=604800, immutable","X-Content-Type-Options":"nosniff"}})}
export default {async fetch(request){
  if(request.method!=="GET"&&request.method!=="HEAD")return new Response("Method not allowed",{status:405,headers:{Allow:"GET, HEAD"}});
  let path=new URL(request.url).pathname;
  try{path=decodeURIComponent(path)}catch{}
  if(path==="/")path="/index.html";
  const asset=assets[path]??assets["/index.html"];
  if(!asset)return new Response("Not found",{status:404});
  if(request.method==="HEAD")return new Response(null,{headers:{"Content-Type":asset.type}});
  return assetResponse(asset,path);
}};
`;

await mkdir(new URL("../dist/server/", import.meta.url), { recursive: true });
await writeFile(new URL("../dist/server/index.js", import.meta.url), output, "utf8");
