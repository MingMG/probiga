"""Execute the navigation check embedded in the production release gate."""
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("variant,valid", [
    ("current", True),
    ("formatted", True),
    ("missing_entry", False),
    ("not_rendered", False),
    ("old_static_sidebar", False),
])
def test_release_checks_actual_dynamic_navigation(tmp_path, variant, valid):
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = deploy.split("verify_strategy_governance_api_and_page_smoke() {", 1)[1]
    body = body.split("verify_strategy_pool_api_and_page_smoke() {", 1)[0]
    checker = body.split('if ! "$BOOTSTRAP_PYTHON" -I - "$app_response" <<\'PY\'\n', 1)[1].split("\nPY\n", 1)[0]
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    if variant == "formatted":
        script = script.replace("id:'strategy-center'", 'id : "strategy-center"')
        script = script.replace("renderSidebar(APP_NAV,", "renderSidebar ( APP_NAV ,")
    elif variant == "missing_entry":
        start = script.index("var APP_NAV = [")
        end = script.index("var TRADING_MODULE_NAV_ITEMS", start)
        script = script[:start] + script[start:end].replace("id:'strategy-center'", "id:'missing-page'") + script[end:]
    elif variant == "not_rendered":
        script = script.replace("renderSidebar(APP_NAV,", "renderSidebar(ANOTHER_NAV,")
    elif variant == "old_static_sidebar":
        script = 'document.body.innerHTML = \'<a data-tab="strategy-center">old title</a>\';'
    page = tmp_path / "app.js"
    page.write_text(script, encoding="utf-8")
    result = subprocess.run([sys.executable, "-I", "-", str(page)], input=checker,
                            encoding="utf-8", capture_output=True, check=False)
    assert (result.returncode == 0) is valid, result.stderr


def test_governance_gate_checks_release_bytes_and_resolved_asset_url():
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = deploy.split("verify_strategy_governance_api_and_page_smoke() {", 1)[1]
    body = body.split("verify_strategy_pool_api_and_page_smoke() {", 1)[0]
    assert 'app_script_url="$(release_page_asset_url' in body
    assert '"http://127.0.0.1$app_script_url"' in body
    assert 'cmp --silent "$PREPARED_CODE_ROOT/server/static/index.html"' in body
    assert 'cmp --silent "$PREPARED_CODE_ROOT/server/static/js/app.js"' in body
    assert 'data-tab="strategy-center"' not in body
    assert "动态策略竞技场" not in body
    assert 'id="tab-strategy-center"' in body
