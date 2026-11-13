import { expect, test } from "bun:test"
import { createAppFixture } from "./fixture/app"
import { tmpdir } from "./fixture/fixture"
import { json } from "./fixture/tui-client"

const unavailable = { status: "unavailable" as const, reason: "database or disk is full", since: 1 }

// The banner follows a server event, not a frame, so wait on the clock rather than a frame budget.
async function waitForFrame(setup: Awaited<ReturnType<typeof createAppFixture>>, matches: (text: string) => boolean) {
  const started = Date.now()
  // A narrow terminal wraps the banner, so compare the text without its line breaks.
  while (!matches(setup.captureCharFrame().replace(/\s+/g, " "))) {
    if (Date.now() - started > 5_000) throw new Error(`Timed out waiting for the frame:\n${setup.captureCharFrame()}`)
    await Bun.sleep(10)
  }
}

test.each([100, 44])("the storage banner shows while writes fail and clears on recovery at width %s", async (width) => {
  await using state = await tmpdir()
  await using setup = await createAppFixture({ width, state: state.path })
  await setup.ready
  await setup.waitForFrame((frame) => frame.includes("commands"))
  expect(setup.captureCharFrame()).not.toContain("Can't save to disk")

  setup.events.emit({ id: "evt_storage_full", created: 1, type: "server.storage.updated", data: unavailable })
  await waitForFrame(setup, (text) => text.includes("Can't save to disk: database or disk is full"))
  // Non-blocking: the home screen is still there underneath.
  expect(setup.captureCharFrame()).toContain("commands")

  setup.events.emit({ id: "evt_storage_ok", created: 2, type: "server.storage.updated", data: { status: "ok" } })
  await waitForFrame(setup, (text) => !text.includes("Can't save to disk"))
})

test("the storage banner shows for a client that connects during the outage", async () => {
  await using state = await tmpdir()
  await using setup = await createAppFixture({
    state: state.path,
    fetch: (url) =>
      url.pathname === "/api/info"
        ? json({ version: "test", pid: 0, urls: [], paths: { tmp: "/tmp" }, storage: unavailable })
        : undefined,
  })
  await setup.ready
  await waitForFrame(setup, (text) => text.includes("Can't save to disk: database or disk is full"))
})

test("the storage banner stays hidden for a blip that recovers within a second", async () => {
  await using state = await tmpdir()
  await using setup = await createAppFixture({ state: state.path })
  await setup.ready
  await setup.waitForFrame((frame) => frame.includes("commands"))

  setup.events.emit({ id: "evt_storage_blip", created: 1, type: "server.storage.updated", data: unavailable })
  setup.events.emit({ id: "evt_storage_back", created: 2, type: "server.storage.updated", data: { status: "ok" } })
  const started = Date.now()
  while (Date.now() - started < 1_500) {
    expect(setup.captureCharFrame()).not.toContain("Can't save to disk")
    await Bun.sleep(50)
  }
})
