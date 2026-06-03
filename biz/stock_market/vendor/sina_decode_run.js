/**
 * 从 stdin 读入密文字符串，输出 d(payload) 的 JSON。
 * 用法: node sina_decode_run.js <decoder.js路径> < payload.txt
 * 或:   echo payload | node sina_decode_run.js <decoder.js路径>
 */
const fs = require("fs");
const vm = require("vm");

const decoderPath = process.argv[2];
if (!decoderPath) {
  console.error("usage: node sina_decode_run.js <sina_kline_decoder.js>");
  process.exit(2);
}
const code = fs.readFileSync(decoderPath, "utf8");
const payload = fs.readFileSync(0, "utf8").trim();
const sandbox = { console };
vm.createContext(sandbox);
vm.runInContext(code, sandbox);
const expr = "d(" + JSON.stringify(payload) + ")";
const out = vm.runInContext(expr, sandbox);
process.stdout.write(JSON.stringify(out));
