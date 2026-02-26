---
description: Create a new commit with auto-generated message and push
allowed-tools: Bash(git:*), Read
---

Do the following steps:

1. Run `git status` and `git diff --stat` to see all changes.
2. Run `git log --oneline -5` to understand recent commit message style.
3. Stage all relevant modified files. Do NOT stage untracked files unless they are clearly part of the work (e.g., new source files). Never stage .env, credentials, or large binaries.
4. Analyze the diff and generate a concise commit message following conventional commits format (feat/fix/refactor/docs/test/chore). Focus on "why" not "what".
5. Create the commit (new commit, NOT amend).
6. Push to the remote branch.
7. Show the final commit hash.

Add `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>` to the commit message.
