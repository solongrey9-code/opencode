export * as Storage from "./storage.js"

import { Schema } from "effect"
import { ephemeral, inventory } from "./event.js"

/** Whether the server can currently save to its database. Kept in memory, so it is known while writes fail. */
export interface Status extends Schema.Schema.Type<typeof Status> {}
export const Status = Schema.Struct({
  status: Schema.Literals(["ok", "unavailable"]),
  reason: Schema.optional(Schema.String).annotate({
    description: "The database's own description of the failure, such as 'database or disk is full'",
  }),
  since: Schema.optional(Schema.Finite).annotate({
    description: "When writes started failing, in milliseconds since the Unix epoch",
  }),
}).annotate({ identifier: "Storage.Status" })

// Published only when the status changes. Ephemeral and unlocated, so delivering it needs no database access.
const Updated = ephemeral({
  type: "server.storage.updated",
  schema: Status.fields,
})
export const Event = { Updated, Definitions: inventory(Updated) }
