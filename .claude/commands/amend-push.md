---
description: Amend the last commit with staged/unstaged changes and force push
allowed-tools: Bash(git:*)
---

Do the following steps:

1. Run `git status` and `git diff --stat` to see what changed.
2. Stage all modified/deleted files (NOT untracked files unless they are clearly part of the current work).
3. Amend the last commit with `--no-edit` (keep existing message).
4. Force push to the remote branch.
5. Show the final commit hash and a one-line summary.

Do NOT create a new commit. Always amend. Always force push. No confirmation needed.
