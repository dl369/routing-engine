# Route — Design Document

Human-readable architecture notes. **Cursor agent rules** live in [`.cursor/rules/`](.cursor/rules/):

| Rule file | Scope |
|-----------|--------|
| [`route-architecture.mdc`](.cursor/rules/route-architecture.mdc) | Always apply — goals, layout, env, coding standards |
| [`gtfs-static.mdc`](.cursor/rules/gtfs-static.mdc) | `internal/models/**`, `internal/gtfs/parser.go` |
| [`gtfs-rt-poller.mdc`](.cursor/rules/gtfs-rt-poller.mdc) | `internal/gtfs/poller.go` |
| [`routing-engine.mdc`](.cursor/rules/routing-engine.mdc) | `internal/router/**`, `internal/rt/**`, `internal/api/**`, `cmd/server/**` |

Edit the `.mdc` files to change what Cursor follows during development.
