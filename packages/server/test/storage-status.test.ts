import path from "node:path"
import { expect } from "bun:test"
import { ClientError, OpenCode } from "@opencode/client"
import { Database } from "@opencode/core/database/database"
import { Global } from "@opencode/util/global"
import { Context, Effect } from "effect"
import { tempGlobalLayer } from "../../core/test/fixture/global"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { serveRoutes } from "./fixture/routes"

const large = "x".repeat(60_000)

const boot = Effect.gen(function* () {
  const dir = yield* tmpdirScoped("opencode-storage-status-")
  const server = yield* serveRoutes(
    {
      app: { version: "test" },
      database: { path: path.join(dir.path, "opencode.db") },
      models: { fetch: false },
      fs: { filewatcher: false },
    },
    [Global.node.replace(tempGlobalLayer)],
  )
  const db = Context.get(server.context, Database.Service).db
  const request = (method: string, route: string, body?: unknown) =>
    Effect.promise(async () => {
      const response = await server.fetch(`http://opencode.local${route}`, {
        method,
        headers: { "content-type": "application/json" },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })
      return { status: response.status, body: await response.text() }
    })
  return {
    db,
    request,
    location: { directory: dir.path },
    api: OpenCode.make({
      baseUrl: "http://opencode.local",
      fetch: Object.assign(
        (input: string | URL | Request, init?: RequestInit) => server.handler(new Request(input, init)),
        { preconnect: fetch.preconnect },
      ),
    }),
    events: yield* Effect.acquireRelease(
      Effect.promise(async () => {
        const response = await server.fetch("http://opencode.local/api/event?types=server.storage.updated")
        if (!response.body) throw new Error("Event response has no body")
        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        const buffer = { pending: "" }
        const next = async (): Promise<{
          readonly type: string
          readonly data: unknown
          readonly location?: unknown
        }> => {
          const boundary = buffer.pending.indexOf("\n\n")
          if (boundary === -1) {
            const chunk = await reader.read()
            if (chunk.done) throw new Error("Event stream closed")
            buffer.pending += decoder.decode(chunk.value, { stream: true })
            return next()
          }
          const frame = buffer.pending.slice(0, boundary)
          buffer.pending = buffer.pending.slice(boundary + 2)
          if (!frame.startsWith("data: ")) return next()
          return JSON.parse(frame.slice(6))
        }
        return { next, close: () => reader.cancel() }
      }),
      (events) => Effect.promise(() => events.close()),
    ),
    /** A real SQLITE_FULL: the database may not grow, so any write that needs a new page fails. */
    fillDisk: Effect.gen(function* () {
      const rows = yield* db.all<{ page_count: number }>("PRAGMA page_count").pipe(Effect.orDie)
      yield* db.run(`PRAGMA max_page_count = ${rows[0]!.page_count}`).pipe(Effect.orDie)
    }),
    freeDisk: db.run("PRAGMA max_page_count = 1073741823").pipe(Effect.orDie),
  }
})

it.live("clients learn that the disk is full, and that it recovered, without a write succeeding", () =>
  Effect.gen(function* () {
    const server = yield* boot
    expect((yield* Effect.promise(() => server.events.next())).type).toBe("server.connected")
    expect((yield* Effect.promise(() => server.api.server.info())).storage).toEqual({ status: "ok" })
    const session = yield* Effect.promise(() => server.api.session.create({ location: server.location }))

    yield* server.fillDisk
    const unavailable = {
      _tag: "StorageUnavailableError",
      message: "Storage error: database or disk is full",
      reason: "database or disk is full",
    }
    const prompt = yield* server.request("POST", `/api/session/${session.id}/prompt`, { text: large })
    expect(prompt.status).toBe(503)
    expect(JSON.parse(prompt.body)).toEqual(unavailable)
    const create = yield* server.request("POST", "/api/session", { location: server.location, title: large })
    expect(create.status).toBe(503)
    expect(JSON.parse(create.body)).toEqual(unavailable)
    // No endpoint declares this status, so the generated client reports it by number, as it does for a
    // starting server. The status event and the banner carry the reason for those clients.
    const thrown = yield* Effect.promise(() =>
      server.api.session.create({ location: server.location, title: large }).then(
        () => undefined,
        (error: unknown) => error,
      ),
    )
    expect(thrown).toBeInstanceOf(ClientError)
    expect(thrown).toMatchObject({ reason: "UnexpectedStatus", cause: { status: 503 } })

    const status = { status: "unavailable" as const, reason: "database or disk is full", since: expect.any(Number) }
    const failed = yield* Effect.promise(() => server.events.next())
    expect(failed).toMatchObject({ type: "server.storage.updated", data: status })
    expect(failed).not.toHaveProperty("location")
    expect((yield* Effect.promise(() => server.api.server.info())).storage).toEqual(status)

    yield* server.freeDisk
    yield* Effect.promise(() => server.api.session.create({ location: server.location, title: large }))
    // One event per change: three failed requests announced a single transition.
    expect(yield* Effect.promise(() => server.events.next())).toMatchObject({
      type: "server.storage.updated",
      data: { status: "ok" },
    })
    expect((yield* Effect.promise(() => server.api.server.info())).storage).toEqual({ status: "ok" })
  }),
)

it.live("a read-only database answers 503 with its reason, and other defects stay 500", () =>
  Effect.gen(function* () {
    const server = yield* boot
    const session = yield* Effect.promise(() => server.api.session.create({ location: server.location }))

    yield* server.db.run("PRAGMA query_only = ON").pipe(Effect.orDie)
    const rename = yield* server.request("PATCH", `/api/session/${session.id}`, { title: "renamed" })
    expect(rename.status).toBe(503)
    expect(JSON.parse(rename.body)).toEqual({
      _tag: "StorageUnavailableError",
      message: "Storage error: attempt to write a readonly database",
      reason: "attempt to write a readonly database",
    })
    yield* server.db.run("PRAGMA query_only = OFF").pipe(Effect.orDie)

    // A query that is simply wrong is a bug, not a storage outage: an empty table shadows the sessions.
    yield* server.db.run("CREATE TEMP TABLE session_v2 (shadow text)").pipe(Effect.orDie)
    const read = yield* server.request("GET", `/api/session/${session.id}`)
    expect(read).toEqual({ status: 500, body: "" })
    yield* server.db.run("DROP TABLE temp.session_v2").pipe(Effect.orDie)
  }),
)
