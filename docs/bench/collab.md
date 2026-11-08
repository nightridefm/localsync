# Collab soak (2026-11-08)

`pnpm browser collab --seconds 120` (`ts/browser/collab.ts`).
Two people (Alice, Bob) edit different projects of one workspace (2
projects x 1000 tasks, 2 comments each) at ~39 transactions/s each: inserts
and deletes of a task with its two comments, single and 20-task bulk
retags; every transaction keeps a task and its comments on one tag. Three
real-browser tabs (localdb on OPFS with a 3-reader pool, synced by the
engine) run a project screen's live queries (two task lists, per-project
and per-task counts, totals, the edited tasks and their comments). After
every render turn a tab checks that the task query and the comment query
agree; latency is from the writer's commit to the render showing it.

| Tab | Render turns | Torn turns | Latency p50 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| Alice, `alignCommits` | 1,137 | 0 | 67 ms | 122 ms | 147 ms |
| Bob, `alignCommits` | 1,138 | 0 | 67 ms | 122 ms | 140 ms |
| Alice, default | 2,335 | 2,163 (93%) | 60 ms | 113 ms | 128 ms |

9,297 transactions in 120 s; every tab converged to Postgres. The default
tab renders twice as often because each reader's results arrive in their
own turn; most of those renders mix two commits.
