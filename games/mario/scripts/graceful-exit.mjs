// Vinext's CLI calls process.exit(0) immediately after its native bundler closes.
// On Windows this can race pending libuv close callbacks. Allow successful builds
// to drain the event loop naturally; preserve every nonzero failure exit.
if (process.platform === 'win32') {
  const exit = process.exit.bind(process);
  process.exit = (code = 0) => {
    if (Number(code) !== 0) return exit(code);
    process.exitCode = 0;
  };
}
