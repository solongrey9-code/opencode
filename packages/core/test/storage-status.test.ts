import { describe, expect, test } from "bun:test"
import path from "path"
import { LanguageModel } from "@opencode/ai/effect"
import { OpenAIChat } from "@opencode/ai/protocols"
import { TestLLM } from "@opencode/ai/testing"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { StorageFailure } from "@opencode/core/database/storage-failure"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNodePlatform } from "@opencode/core/effect/app-node-platform"
import { Job } from "@opencode/core/job"
import { Location } from "@opencode/core/location"
import { LocationServiceMap } from "@opencode/core/location-service-map"
import { Model } from "@opencode/core/model"
import { Plugin } from "@opencode/core/plugin"
import { PluginSupervisor } from "@opencode/core/plugin/supervisor"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { SessionStore } from "@opencode/core/session/store"
import { toSessionError } from "@opencode/core/session/to-session-error"
import { StorageStatus } from "@opencode/core/storage-status"
import type { Event } from "@opencode/schema/event"
import { SessionEvent } from "@opencode/schema/session-event"
import { Storage } from "@opencode/schema/storage"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { Cause, Context, Effect, Exit, Fiber, Layer, Stream } from "effect"
import { tempGlobalLayer } from "./fixture/global"
import { offlineModels } from "./fixture/models"
import { tmpdirScoped } from "./fixture/tmpdir"
import { it } from "./lib/effect"

type Db = Database.Interface["db"]

/**
 * A real SQLITE_FULL: the database may not grow past its current size plus `spare` pages. Writes
 * that need more fail with "database or disk is full"; writes that fit still land.
 */
const fillDisk = (db: Db, spare = 0) =>
  Effect.gen(function* () {
    const rows = yield* db.all<{ page_count: number }>("PRAGMA page_count").pipe(Effect.orDie)
    yield* db.run(`PRAGMA max_page_count = ${rows[0]!.page_count + spare}`).pipe(Effect.orDie)
  })
const freeDisk = (db: Db) => db.run("PRAGMA max_page_count = 1073741823").pipe(Effect.orDie)
/** Every write fails with SQLITE_READONLY; reads keep working. */
const breakWrites = (db: Db) => db.run("PRAGMA query_only = ON").pipe(Effect.orDie)
const healWrites = (db: Db) => db.run("PRAGMA query_only = OFF").pipe(Effect.orDie)

const large = "x".repeat(60_000)

const storageEvents = (bus: Bus.Interface) =>
  Effect.gen(function* () {
    const events: Event.Payload<typeof Storage.Event.Updated>[] = []
    yield* bus.listen((event) =>
      Effect.sync(() => {
        if (event.type === Storage.Event.Updated.type) events.push(event as Event.Payload<typeof Storage.Event.Updated>)
      }),
    )
    return events
  })

const bootBus = Effect.gen(function* () {
  const tmp = yield* tmpdirScoped()
  const context = yield* Layer.build(
    AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, StorageStatus.node]), [
      Database.node.replace(Database.configured({ path: path.join(tmp.path, "storage.sqlite") })),
      Bus.node.replace(Bus.configured({ persist: true })),
      Global.node.replace(tempGlobalLayer),
    ]),
  )
  const bus = Context.get(context, Bus.Service)
  const sessionID = Session.ID.create()
  return {
    bus,
    db: Context.get(context, Database.Service).db,
    storage: Context.get(context, StorageStatus.Service),
    events: yield* storageEvents(bus),
    // A persisted event row is as large as its payload, so a large title needs new pages.
    write: (title: string) => bus.publish(SessionEvent.Renamed, { sessionID, title }).pipe(Effect.exit),
  }
})

describe("storage failure", () => {
  test("recognises each driver's error shape and ignores failures that are not the database's", () => {
    // bun:sqlite names the result code, extended ones included.
    expect(StorageFailure.find({ code: "SQLITE_IOERR_WRITE", errno: 778, message: "disk I/O error" })).toEqual({
      code: "SQLITE_IOERR",
      reason: "disk I/O error",
    })
    // node:sqlite reports the number, whose low byte is the primary code.
    expect(
      StorageFailure.find({ code: "ERR_SQLITE_ERROR", errcode: 8, message: "attempt to write a readonly database" }),
    ).toEqual({ code: "SQLITE_READONLY", reason: "attempt to write a readonly database" })
    // A write that waited out busy_timeout is lost like any other, so a locked database counts.
    expect(StorageFailure.find(new Error("outer", { cause: { code: "SQLITE_BUSY", message: "" } }))).toEqual({
      code: "SQLITE_BUSY",
      reason: "database is locked",
    })
    expect(StorageFailure.find({ code: "SQLITE_CONSTRAINT_PRIMARYKEY", message: "UNIQUE constraint failed" })).toBe(
      undefined,
    )
    expect(StorageFailure.find({ code: "ENOSPC", errno: -28, message: "no space left on device" })).toBeUndefined()
    expect(StorageFailure.find(new Error("plain"))).toBeUndefined()
  })
})

describe("storage status", () => {
  it.live("a full disk is announced once, and recovery once", () =>
    Effect.gen(function* () {
      const app = yield* bootBus
      expect(yield* app.storage.current).toEqual({ status: "ok" })
      expect(Exit.isSuccess(yield* app.write("fits"))).toBe(true)
      expect(app.events).toEqual([])

      yield* fillDisk(app.db)
      const failed = yield* app.write(large)
      expect(Exit.isFailure(failed)).toBe(true)
      const unavailable = yield* app.storage.current
      expect(unavailable).toEqual({
        status: "unavailable",
        reason: "database or disk is full",
        since: expect.any(Number),
      })
      expect(app.events.map((event) => event.data)).toEqual([unavailable])
      // Unlocated and not durable: delivering it needs neither a route lookup nor a commit.
      expect(app.events[0]).not.toHaveProperty("location")
      expect(app.events[0]).not.toHaveProperty("durable")

      expect(Exit.isFailure(yield* app.write(large))).toBe(true)
      expect(Exit.isFailure(yield* app.write(large))).toBe(true)
      expect(app.events).toHaveLength(1)
      expect(yield* app.storage.current).toEqual(unavailable)

      yield* freeDisk(app.db)
      expect(Exit.isSuccess(yield* app.write(large))).toBe(true)
      expect(yield* app.storage.current).toEqual({ status: "ok" })
      expect(app.events.map((event) => event.data)).toEqual([unavailable, { status: "ok" }])

      expect(Exit.isSuccess(yield* app.write("again"))).toBe(true)
      expect(app.events).toHaveLength(2)
    }),
  )

  it.live("a read-only database reports its own reason, and subscribers receive the change", () =>
    Effect.gen(function* () {
      const app = yield* bootBus
      const received = yield* app.bus
        .subscribe(Storage.Event.Updated)
        .pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped({ startImmediately: true }))

      yield* breakWrites(app.db)
      expect(Exit.isFailure(yield* app.write("small"))).toBe(true)
      expect(yield* app.storage.current).toMatchObject({
        status: "unavailable",
        reason: "attempt to write a readonly database",
      })
      yield* healWrites(app.db)
      expect(Exit.isSuccess(yield* app.write("small"))).toBe(true)

      expect((yield* Fiber.join(received)).map((event) => event.data.status)).toEqual(["unavailable", "ok"])
    }),
  )

  it.live("a failure that is not the database's leaves the status alone", () =>
    Effect.gen(function* () {
      const app = yield* bootBus
      yield* app.bus.project(SessionEvent.Renamed, () => Effect.die(new Error("projector bug")))
      expect(Exit.isFailure(yield* app.write("small"))).toBe(true)
      expect(yield* app.storage.current).toEqual({ status: "ok" })
      expect(app.events).toEqual([])
    }),
  )

  it.live("the reason is found under a failed statement, and only for storage failures", () =>
    Effect.gen(function* () {
      const app = yield* bootBus
      yield* app.db.run("create table filler (id integer primary key, value text)").pipe(Effect.orDie)
      const syntax = yield* app.db.run("selec").pipe(Effect.flip)
      expect(StorageFailure.find(syntax)).toBeUndefined()
      expect(toSessionError(syntax).type).toBe("unknown")

      yield* fillDisk(app.db)
      const full = yield* app.db.run(`insert into filler (value) values ('${large}')`).pipe(Effect.flip)
      // The statement error's own message is the SQL, which is what clients used to be shown.
      expect(full.message).toStartWith("Failed query:")
      expect(toSessionError(full)).toEqual({ type: "storage", message: "Storage error: database or disk is full" })
    }),
  )

  it.live("a transaction SQLite already rolled back still fails with the full disk, not the rollback", () =>
    Effect.gen(function* () {
      const app = yield* bootBus
      yield* app.db.run("create table filler (id integer primary key, value text)").pipe(Effect.orDie)
      yield* fillDisk(app.db)
      const exit = yield* app.db
        .transaction(() => app.db.run(`insert into filler (value) values ('${large}')`), { behavior: "immediate" })
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      expect(Cause.pretty(exit.cause)).not.toContain("cannot rollback")
      expect(StorageFailure.find(exit.cause)).toEqual({ code: "SQLITE_FULL", reason: "database or disk is full" })
      expect(toSessionError(Cause.squash(exit.cause))).toEqual({
        type: "storage",
        message: "Storage error: database or disk is full",
      })
      // The connection is usable again: no transaction was left open.
      yield* freeDisk(app.db)
      yield* app.db.transaction(() => app.db.run("insert into filler (value) values ('ok')")).pipe(Effect.orDie)
    }),
  )
})

const bootSessions = Effect.gen(function* () {
  const tmp = yield* tmpdirScoped()
  const directory = yield* tmpdirScoped()
  const llmContext = yield* Layer.build(TestLLM.testLayer({ fallback: TestLLM.text("unexpected", "unexpected") }))
  const context = yield* Layer.build(
    AppNodeBuilder.build(
      LayerNode.group([
        Database.node,
        Bus.node,
        Job.node,
        Session.node,
        SessionExecution.node,
        LocationServiceMap.node,
        SessionStore.node,
        StorageStatus.node,
      ]),
      [
        Database.node.replace(Database.configured({ path: path.join(tmp.path, "storage.sqlite") })),
        Global.node.replace(tempGlobalLayer),
        offlineModels,
        PluginSupervisor.node.replace(
          makeLocationNode({ name: "test/no-plugins", layer: Layer.empty, deps: [Agent.node] }),
        ),
        LayerNodePlatform.llmClient.replace(Layer.succeedContext(llmContext)),
        SessionRunnerModel.node.replace(
          Layer.succeed(
            SessionRunnerModel.Service,
            SessionRunnerModel.Service.of({
              resolve: (_session, _available, request) =>
                Effect.succeed(
                  SessionRunnerModel.resolved(
                    LanguageModel.make({ id: request.agent, provider: "test", route: OpenAIChat.route }),
                    {
                      capabilities: { tools: true, input: ["text"], output: ["text"] },
                      cost: [],
                      limit: { context: 200_000, output: 32_000 },
                    },
                  ),
                ),
            }),
          ),
        ),
      ],
    ),
  )
  const location = Location.Ref.make({ directory: AbsolutePath.make(directory.path) })
  const locations = Context.get(context, LocationServiceMap.Service)
  yield* Plugin.Service.use((plugins) => plugins.awaitActivation).pipe(Effect.provide(locations.get(location)))
  yield* Agent.Service.use((agents) =>
    agents.transform((editor) =>
      editor.update(Agent.ID.make("build"), (agent) => {
        agent.mode = "primary"
      }),
    ),
  ).pipe(Effect.provide(locations.get(location)))
  const sessions = Context.get(context, Session.Service)
  const bus = Context.get(context, Bus.Service)
  return {
    bus,
    sessions,
    db: Context.get(context, Database.Service).db,
    execution: Context.get(context, SessionExecution.Service),
    storage: Context.get(context, StorageStatus.Service),
    llm: Context.get(llmContext, TestLLM.Test),
    events: yield* storageEvents(bus),
    session: yield* sessions.create({
      location,
      model: Model.Ref.make({ id: Model.ID.make("parent"), providerID: Provider.ID.make("test") }),
    }),
  }
})

describe("storage failures during a session", () => {
  it.live(
    "a step that cannot be saved fails the session with the storage reason",
    () =>
      Effect.gen(function* () {
        const app = yield* bootSessions
        const failures: SessionEvent.Execution.Failed["data"]["error"][] = []
        yield* app.bus.listen((event) =>
          Effect.sync(() => {
            if (event.type === SessionEvent.Execution.Failed.type)
              failures.push((event as Event.Payload<typeof SessionEvent.Execution.Failed>).data.error)
          }),
        )
        // The disk fills once the provider starts answering. The answer is too large to save, while the
        // few spare pages let the small failure records land, as they did in the original incident.
        yield* app.llm.serve(() =>
          Stream.unwrap(fillDisk(app.db, 8).pipe(Effect.as(Stream.fromIterable(TestLLM.text(large, "answer"))))),
        )
        yield* app.sessions.prompt({ sessionID: app.session.id, text: "hello" })
        yield* app.execution.awaitIdle(app.session.id)

        const error = { type: "storage", message: "Storage error: database or disk is full" }
        expect(failures).toEqual([error])
        // The transcript keeps it, so a client that connects later still sees why the session stopped.
        expect((yield* app.sessions.context(app.session.id)).at(-1)).toMatchObject({ type: "idle", error })
        expect(app.events.map((event) => event.data.status)).toEqual(["unavailable", "ok"])
        expect(app.events[0]?.data.reason).toBe("database or disk is full")
      }),
    30_000,
  )
})
