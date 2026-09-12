# Windows 开发仓库 Git 推送

本机开发仓库使用 GitHub SSH 443 推送，避免本地 HTTP 代理端口变化或代理程序退出反复阻断提交。配置保存于开发仓库的公共 Git 配置，现有 worktree 共同使用；生产克隆没有配置此写入身份。

## 配置约定

| 配置 | 值或行为 |
| --- | --- |
| `remote.origin.url` | `https://github.com/MingMG/probiga.git`，保留发布校验使用的规范地址 |
| `remote.origin.pushurl` | `ssh://git@ssh.github.com:443/MingMG/probiga.git` |
| `core.sshCommand` | 显式指定 Git for Windows 自带的 OpenSSH 和本机专用私钥 |
| GitHub 写入身份 | 仅授予 `MingMG/probiga` 的仓库部署密钥；标题为 `ProBigA Windows dedicated Git write identity` |
| 私钥存放 | 当前 Windows 用户的 `.ssh/probiga_github_write_ed25519`，文件访问仅限该用户 |

SSH 命令使用 `-F none`、`IdentitiesOnly=yes`、`IdentityAgent=none`、`BatchMode=yes`、`ProxyCommand=none` 和 `ProxyJump=none`。正常 `git push` 直接连接 `ssh.github.com:443`，无需 HTTP 代理、SSH agent 或交互输入密码。连接超时为 15 秒，连接尝试一次，并启用存活检查。

主机认证启用 `StrictHostKeyChecking=yes`，显式指定当前用户的 `known_hosts`，设置 `GlobalKnownHostsFile=none`、`HostKeyAlgorithms=ssh-ed25519` 和 `UpdateHostKeys=no`。登记的 `[ssh.github.com]:443` Ed25519 指纹为 `SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU`，与 [GitHub 官方指纹](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints)一致。端点见 [GitHub SSH 443 文档](https://docs.github.com/en/authentication/troubleshooting-ssh/using-ssh-over-the-https-port)。

`fetch` 仍使用 HTTPS，并依赖本机现有的 GitHub 专用 HTTP 代理配置。本次推送配置不改变生产下载、调度器或 QMT 的运行方式。

## 验证和维护

在开发 worktree 中运行以下命令，分别检查读取地址、推送地址和保存的完整 SSH 命令：

```powershell
git remote get-url origin
git remote get-url --push origin
git config --show-origin --get core.sshCommand
git push --dry-run
```

发布前按项目分支流程提交并执行普通 `git push`，随后通过推送端点回读分支 SHA，与本地 `git rev-parse HEAD` 比较。`--dry-run` 用于检查连接和权限，不能替代真实提交的远端回读。

Windows 设置 `core.sshCommand` 时须完整保留含空格的 OpenSSH 路径及命令参数；写入后逐字回读验证，不能仅以配置命令退出码判断成功。避免 `GIT_SSH_COMMAND`、`GIT_SSH` 或 worktree 专属配置覆盖仓库设置。

迁移机器或 Windows 用户时，应重新登记该机器的仓库专用密钥，并核验主机指纹与私钥访问权限。私钥、GitHub 令牌及应用登录密码均不进入仓库。撤销此机器写入权限时，在仓库的 Deploy keys 中删除对应密钥，并清理本机 `remote.origin.pushurl` 和 `core.sshCommand` 配置。

本配置影响 Windows 开发环境的 Git 推送，不涉及生产代码发布或服务重启。网络中断、GitHub 故障或密钥被撤销仍可能造成推送失败。
