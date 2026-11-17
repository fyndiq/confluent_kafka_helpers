<!-- baseline v1 start -->
## Org baseline

Shared by every fyndiq repo and synced from `baseline/AGENTS.baseline.md` in `fyndiq/.github`. Change it there by PR, never between these markers.

Never:
- Promote or deploy to prod unless a human asked for prod in this conversation.
- Commit secrets, tokens, keys or customer data.
- Push to the default branch or force-push a shared branch. Every change goes through a PR, and you merge one only where the org's rules give an agent that right.
- Skip, disable or edit a check, hook or workflow to get a change through (`--no-verify`, a deleted test, an edited required workflow).
- Change a production database, Kafka topic or cloud resource by hand. Go through the service, its migrations or `fyndiq/infrastructure`.

Every change:
- Is verified the way this file says below, and the PR carries the evidence: command output, run URL or screenshot.
- Has no comments that restate the code, no guards the types already give and no magic numbers.

Harness plugin skills, load the one that matches the task:
- `deploy`: promote a branch to integration or master to prod, redeploy, revert, see what is running where.
- `review`: review a change locally with the PR review bot's rules before pushing.
- `break-glass`: time-boxed GitHub access during an incident.

How the department works (ways of working, architecture decisions, incident process, recipes): https://github.com/fyndiq/docs
<!-- baseline v1 end -->
