// you might need to change this path to your package.json
import { bin } from "./package.json";
import { execSync } from "child_process";

// Compute the target we're building for
const bunTargets: Record<string, string> = {
  "x86_64-pc-windows-msvc": "bun-windows-x64",
  "aarch64-apple-darwin": "bun-darwin-arm64",
  "x86_64-apple-darwin": "bun-darwin-x64",
  "aarch64-unknown-linux-gnu": "bun-linux-arm64",
  "x86_64-unknown-linux-gnu": "bun-linux-x64",
} as const;
const distTarget = process.env.CARGO_DIST_TARGET || process.env.DIST_TARGET;
if (!distTarget) {
  throw "DIST_TARGET isn't set, so we don't know what platform to build!";
}
const bunTarget = bunTargets[distTarget];
if (!bunTarget) {
  throw `To the the best of our knowledge, bun does not support building for ${distTarget}`;
}
const binExt = distTarget.includes("windows") ? ".exe" : "";

// setup bun
execSync("bun install");

for (const [binName, binScript] of Object.entries(bin)) {
  const binPath = `${binName}${binExt}`;
  execSync(`bun build --compile --minify --sourcemap --bytecode --format esm --target ${bunTarget} --outfile ${binPath} ${binScript}`);
}
