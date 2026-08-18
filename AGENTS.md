# Codex branch and release workflow

For every new Codex conversation that changes this project:

1. Fetch the latest production `main` branch before editing.
2. Create a new, conversation-specific `codex/*` branch from that exact `main` revision.
3. Never make code changes directly on `main`, and never reuse a branch from another conversation.
4. Implement and test all changes on the conversation branch.
5. Commit and push the conversation branch, merge it into `main`, then deploy only the merged `main` revision.
6. Keep unrelated user changes out of the branch; use a separate worktree when the current tree is dirty.

Read-only investigation does not require a branch until a file change is needed.
