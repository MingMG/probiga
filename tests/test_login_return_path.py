from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_login_return_path_uses_browser_url_origin_and_preserves_encoded_questions():
    source = (ROOT / "server/static/js/login.js").read_text(encoding="utf-8")
    start = source.index("  function safeNext()")
    function = source[start:source.index("  function setError", start)]
    harness = "const assert=require('assert');\n" + function + r"""
const window={location:{origin:'https://probiga.example',search:''}};
function next(value){window.location.search='?next='+encodeURIComponent(value);return safeNext();}
assert.strictEqual(next('/\\evil.example/path'),'/');
assert.strictEqual(next('//evil.example/path'),'/');
assert.strictEqual(next('https://evil.example/path'),'/');
assert.strictEqual(next('https://probiga.example//evil.example/path'),'/');
assert.strictEqual(new URL(next('https://probiga.example//evil.example/path'),window.location.origin).origin,window.location.origin);
assert.strictEqual(next('javascript:alert(1)'),'/');
assert.strictEqual(next('/login'),'/');
assert.strictEqual(next('/%6cogin?next=/'),'/');
const research='/ai-stock?stock_code=000001&question='+encodeURIComponent('涨幅 10% & 风险？')+'#draft';
assert.strictEqual(next(research),research);
assert.strictEqual(next('/?tab=portfolio&stock_code=600519'),'/?tab=portfolio&stock_code=600519');
assert.strictEqual(new URL(next('/\\evil.example/path'),window.location.origin).origin,window.location.origin);
"""
    result = subprocess.run([shutil.which("node"), "-e", harness], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    assert result.returncode == 0, result.stdout + result.stderr
