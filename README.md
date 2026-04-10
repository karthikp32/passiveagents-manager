# PassiveAgents Manager

The PassiveAgents manager is the local daemon and CLI that runs user-approved coding agents on a machine, keeps them attached to tasks, and streams their terminal output back to PassiveAgents.

## License

This repository is released under `BSL 1.1`.

The intent of this public repo is auditability and transparency. Reuse is governed by the Business Source License terms for this repository.

## Scope

This repo contains the extracted manager code, not the full PassiveAgents product.

Included here:

- the `passiveagents` CLI entrypoint
- the local manager runtime
- service-install helpers for running the manager in the background
- focused unit and subprocess tests for manager behavior

Not included here:

- the PassiveAgents web app
- backend APIs
- release automation from the original monorepo

## Repository Status

This is a standalone extraction from the main PassiveAgents codebase. The root docs and module metadata in this repo are intentionally scoped to the manager only.

The current source tree is Unix-only. The main Go files use `//go:build unix`, so this checkout is intended for macOS and Linux auditing, building, and testing.

## Layout

```text
cmd/passiveagents/
  main.go              CLI and manager runtime
  service.go           launchd/systemd/task setup helpers
  realtime.go          websocket/browser streaming support
  vt_state.go          terminal replay and screen state helpers
  *_test.go            focused unit and subprocess coverage
```

## Requirements

To run the manager locally you currently need:

- Go `1.24+`
- `cloudflared` on `PATH`, or `PASSIVEAGENTS_CLOUDFLARED_PATH` set explicitly
- a PassiveAgents account and reachable PassiveAgents deployment
- at least one supported coding-agent runtime installed locally

## Build

From the repo root:

```bash
go build -o passiveagents ./cmd/passiveagents
```

## Test

Run the manager test suite from the repo root:

```bash
go test ./...
```

## Local Usage

Run the CLI directly from source:

```bash
go run ./cmd/passiveagents version
go run ./cmd/passiveagents login --url https://passiveagents.com
go run ./cmd/passiveagents start
go run ./cmd/passiveagents status
go run ./cmd/passiveagents tasks
go run ./cmd/passiveagents list agents
go run ./cmd/passiveagents manager logs
go run ./cmd/passiveagents stop
```

`passiveagents start` installs or refreshes per-user background-service integration and starts the long-lived manager under that supervisor.

## Commands

The current CLI includes commands for:

- authentication: `login`
- lifecycle: `start`, `stop`, `status`, `version`
- tasks: `tasks`, `reset <task-id>`
- agent inspection and chat: `list agents`, `chat agent <AGENT_INSTANCE_ID>`, `agent logs <agent-instance-id>`
- folder allowlisting: `add folder <folder-path>`, `remove folder <folder-id-or-path>`
- manager logs: `manager logs`

## Notes For Auditors

- This repo is intended to expose the local manager implementation cleanly.
- Generated binaries and local editor/tooling artifacts are not part of source control.
- If you are reviewing service install behavior, start with [cmd/passiveagents/service.go](cmd/passiveagents/service.go).
- If you are reviewing task execution and streaming behavior, start with [cmd/passiveagents/main.go](cmd/passiveagents/main.go) and [cmd/passiveagents/realtime.go](cmd/passiveagents/realtime.go).
