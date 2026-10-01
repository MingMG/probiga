"""Exercise PS5 parsing and the real offline-package integrity gate, no installs."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / 'tools' / 'secondary_edge'
PS = shutil.which('powershell.exe')
pytestmark = pytest.mark.skipif(os.name != 'nt' or not PS, reason='Windows PowerShell 5.1 required')


def run_ps(code):
    return subprocess.run([PS, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                           '-Command', "$ErrorActionPreference='Stop';" + code],
                          capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize('name', ['package_common', 'prepare_cold_package', 'export_cold_package',
                                 'pause_source', 'migrate_target', 'target_entry'])
def test_actual_windows_powershell_parse(name):
    path = str(TOOLS / (name + '.ps1')).replace("'", "''")
    result = run_ps("$t=$null;$e=$null;[System.Management.Automation.Language.Parser]::ParseFile('"
                    + path + "',[ref]$t,[ref]$e)|Out-Null;if($e.Count){throw ($e|Out-String)}")
    assert result.returncode == 0, result.stderr


def package(tmp_path):
    paths = ['code.bundle', 'database/data/mysql.ibd', 'database/data/auto.cnf', 'database/config/my.ini',
             'database/metadata.json', 'database/certs/ca.pem','database/certs/server-cert.pem','database/certs/server-key.pem',
             'audit/source-pause.json','audit/source-project-state/archive-metadata.json',
             'audit/source-project-state/development/data/main.db', 'package_common.ps1','migrate_target.ps1','target_entry.ps1',
             'start_target_migration.cmd','COLD_README.txt', 'wheels313/fixture.whl','wheels314/fixture.whl',
             'software/qmt/bin.x64/XtItClient.exe', 'software/mysql84/bin/mysqld.exe',
             'software/mysql84/bin/mysql.exe','software/mysql84/bin/mysqladmin.exe',
             'software/codex/codex.exe', 'software/installers/python313.exe',
             'software/codex/codex-code-mode-host.exe','software/codex/codex-command-runner.exe',
             'software/codex/codex-windows-sandbox-setup.exe',
             'software/installers/python314.exe', 'software/installers/git.exe',
             'software/installers/vc_x64.exe', 'software/installers/vc_x86.exe',
             'software/installers/chrome.msi']
    files = []
    for name in paths:
        file = tmp_path / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(b'package-fixture')
        if name == 'audit/source-pause.json':
            file.write_text(json.dumps({'format':'probiga.source-pause.v1','status':'paused','source_host':'source-fixture',
                'source_service_state':'Stopped','source_service_startup':'Disabled','shutdown_complete':True,
                'source_processes_running':False,'source_qmt_running':False,'source_automatically_resume':False,
                'source_server_uuid':'fixture-uuid'}), encoding='utf-8')
        files.append({'path': name.replace('/', '\\'), 'bytes': file.stat().st_size,
                      'sha256': hashlib.sha256(file.read_bytes()).hexdigest()})
    manifest = {'format': 'probiga.windows-cold-migration.v2', 'build_sha': 'a'*40,
                'source_host': 'source-fixture', 'production_activation': False,
                'source_paused':True,'restore_requested':False,'database':{'source':{'server_uuid':'fixture-uuid'}},
                'minimum_target_free_bytes': 250*1024**3, 'files': files}
    return manifest


def verify(tmp_path, manifest):
    (tmp_path / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    if not (tmp_path / 'READY').exists():
        seal=hashlib.sha256((tmp_path / 'manifest.json').read_bytes()).hexdigest().upper()
        (tmp_path / 'READY').write_text('a'*40+' '+seal, encoding='utf-8')
    common = str(TOOLS / 'package_common.ps1').replace("'", "''")
    path = str(tmp_path).replace("'", "''")
    return run_ps(f". '{common}';Assert-ColdPackage '{path}'|Out-Null")


def test_complete_package_passes(tmp_path):
    assert verify(tmp_path, package(tmp_path)).returncode == 0


def test_corrupt_required_file_is_rejected(tmp_path):
    manifest = package(tmp_path)
    (tmp_path / 'code.bundle').write_bytes(b'corrupted')
    assert verify(tmp_path, manifest).returncode != 0


def test_omitted_database_is_rejected(tmp_path):
    manifest = package(tmp_path)
    manifest['files'] = [item for item in manifest['files'] if item['path'] != 'database\\data\\mysql.ibd']
    assert verify(tmp_path, manifest).returncode != 0


def test_traversal_is_rejected(tmp_path):
    manifest = package(tmp_path)
    manifest['files'].append({'path': '..\\outside.sql', 'bytes': 0, 'sha256': '0'*64})
    assert verify(tmp_path, manifest).returncode != 0


def test_mismatched_ready_is_rejected(tmp_path):
    manifest = package(tmp_path)
    (tmp_path / 'READY').write_text('b'*40, encoding='utf-8')
    assert verify(tmp_path, manifest).returncode != 0
