# 接入 GitHub 需求跟踪与工程验收证据

登记状态：待同步。2026-11-08 核查 `MingMG/probiga`，没有找到匹配的 Issue。GitHub 集成创建 Issue 返回 HTTP 403 `Resource not accessible by integration`。

## 需求摘要

在现有工程验收控制台关联 GitHub Issue、PR 和交付证据，在 ProBigA 导航加入可直接打开独立站点的入口。

## 验收条件

- 无 PR 的 Issue 也可查，支持一对多关联与项目、编号、日期、进度及发布分类筛选。
- 独立存储保留上次成功同步结果，旧浏览器证据可重复导入且原始记录不被覆盖。
- 独立显示开发合并、测试、实际发布和生产读回，证据缺失或不匹配时显示具体原因。
- #87 分别核对同一候选提交与环境的回归和 HTTP 报告。#85 只评估 Windows/QMT 单端发布。

## 影响范围

ProBigA Linux/server 导航、开发登记流程和现有独立控制台。生产操作均为待审批步骤。

## 当前进展

工作分支 `codex/issue-evidence-console-20261108`。历史 PR #87 和 #85 用于验收，实施 PR 另行关联本需求。

## 当前阻塞

当前 GitHub 集成缺少 Issue 写权限。生产发布、Windows/QMT 授权目标及生产读回证据需另行核验。

## 下一步

取得 `Issues: write` 后重新查重并登记，用 `Refs #<Issue 编号>` 关联实施 PR。控制台发布与生产验收保留为待审批。
