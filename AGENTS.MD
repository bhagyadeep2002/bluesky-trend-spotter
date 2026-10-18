# Repository instructions

- Read `PROJECT_CONTEXT.MD` and the project brief before changing the project.
- Maintain an up-to-date `PROJECT_CONTEXT.MD`. Whenever a code, configuration,
  dependency, workflow, or documentation change makes it outdated, update it in
  the same change. Record the current milestone, architecture, commands,
  validation results, limitations, decisions, and next task accurately.
- Maintain a Git repository and commit all completed project changes. Check the
  working tree before and after committing; never commit credentials, local
  environments, generated raw events, or caches. If environment permissions
  prevent normal Git operations, document the limitation and preserve committed
  history in a portable Git bundle when possible.
- Implement one milestone at a time; preserve useful existing work.
- Use deterministic offline tests for ingestion, including failure paths. Run
  relevant tests for requested implementation and record actual results.
- Never claim live capture, durability, performance, or recovery guarantees
  beyond what has been tested. Do not log post payloads or credentials.
