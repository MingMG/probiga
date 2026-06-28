$ErrorActionPreference = "Stop"

Get-CimInstance Win32_Process | Where-Object {
    (
        $_.Name -eq "python.exe" -and (
            $_.CommandLine -like "*run_qmt_live_runtime.py*" -or
            $_.CommandLine -like "*run_remote_mysql_tunnel.py*"
        )
    ) -or (
        $_.Name -eq "powershell.exe" -and (
            $_.CommandLine -like "*run_local_live_supervisor.ps1*" -or
            $_.CommandLine -like "*launch_local_live_supervisor.ps1*"
        )
    )
} | ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}
