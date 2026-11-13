export * as StorageStatus from "./storage-status.js"

import type { Storage } from "@opencode/schema/storage"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Clock, Context, Effect, Layer, Ref } from "effect"
import type { Cause } from "effect"
import { StorageFailure } from "./database/storage-failure.js"

export interface Interface {
  readonly current: Effect.Effect<Storage.Status>
  /**
   * Records a failed write. Returns the new status only when this changed it, so callers announce
   * transitions instead of every failure. A cause that is not a storage failure changes nothing.
   */
  readonly failed: (cause: Cause.Cause<unknown>) => Effect.Effect<Storage.Status | undefined>
  /** Records a committed write. Returns the new status only when this changed it. */
  readonly recovered: Effect.Effect<Storage.Status | undefined>
}

/** Whether the database can be written, held in memory so it stays readable while writes fail. */
export class Service extends Context.Service<Service, Interface>()("@opencode/StorageStatus") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const state = yield* Ref.make<Storage.Status>({ status: "ok" })
    return Service.of({
      current: Ref.get(state),
      failed: Effect.fnUntraced(function* (cause) {
        const failure = StorageFailure.find(cause)
        if (!failure) return undefined
        const since = yield* Clock.currentTimeMillis
        const changed = yield* Ref.modify(state, (current): [Storage.Status | undefined, Storage.Status] => {
          if (current.status === "unavailable") return [undefined, current]
          const next = { status: "unavailable" as const, reason: failure.reason, since }
          return [next, next]
        })
        if (changed) yield* Effect.logError("Storage unavailable", { reason: failure.reason, code: failure.code })
        return changed
      }),
      recovered: Ref.modify(state, (current): [Storage.Status | undefined, Storage.Status] => {
        if (current.status === "ok") return [undefined, current]
        const next = { status: "ok" as const }
        return [next, next]
      }).pipe(Effect.tap((changed) => (changed ? Effect.logInfo("Storage recovered") : Effect.void))),
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [] })
