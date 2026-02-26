---
description: Deep-dive explanation of a MynaDB module or subsystem
argument-hint: <module-path-or-name>
allowed-tools: Read, Glob, Grep, Task
model: sonnet
---

Provide a deep-dive explanation of the MynaDB module: $ARGUMENTS

Steps:
1. Find the module files (search by name or path in `src/`).
2. Read all relevant source files thoroughly.
3. Explain:
   - **Purpose**: What problem does this module solve?
   - **Data flow**: How does data enter and leave this module?
   - **Key types**: Main structs, enums, traits and their relationships
   - **Public API**: What functions/methods are exposed?
   - **Integration**: How does it connect to other modules?
   - **Configuration**: What config options affect this module?
   - **Concurrency**: Any locking, async patterns, or thread safety considerations?
4. Include a text-based architecture diagram if helpful.

Be concise but thorough. This is for understanding the code, not modifying it.
