export * as StorageFailure from "./storage-failure.js"

import { Cause } from "effect"

/**
 * A SQLite failure that means the database itself can't be used, as opposed to a bad statement or a
 * constraint. `reason` is SQLite's own text for it, which is the only part worth showing a person.
 */
export interface Info {
  readonly code: Code
  readonly reason: string
}

// SQLite primary result codes and their `sqlite3_errstr` text, used when the driver error has no message.
// BUSY and LOCKED count: they only surface once a write has already waited out `busy_timeout`, and the
// write is lost just like on a full disk.
const codes = {
  SQLITE_BUSY: { primary: 5, reason: "database is locked" },
  SQLITE_LOCKED: { primary: 6, reason: "database table is locked" },
  SQLITE_READONLY: { primary: 8, reason: "attempt to write a readonly database" },
  SQLITE_IOERR: { primary: 10, reason: "disk I/O error" },
  SQLITE_CORRUPT: { primary: 11, reason: "database disk image is malformed" },
  SQLITE_FULL: { primary: 13, reason: "database or disk is full" },
  SQLITE_CANTOPEN: { primary: 14, reason: "unable to open database file" },
  SQLITE_NOTADB: { primary: 26, reason: "file is not a database" },
} as const

export type Code = keyof typeof codes

const names = Object.keys(codes) as Code[]

export function message(info: Info) {
  return `Storage error: ${info.reason}`
}

/**
 * Finds the storage failure behind an error or a Cause, however many wrappers sit above it. A failed
 * query is a query error holding a Cause holding a SqlError holding its reason holding the driver error.
 */
export function find(error: unknown) {
  return search(error, 0)
}

function search(error: unknown, depth: number): Info | undefined {
  // The depth bound also stops a cause chain that loops back on itself.
  if (typeof error !== "object" || error === null || depth > 10) return undefined
  if (Cause.isCause(error))
    return error.reasons
      .map((reason) => {
        if (Cause.isFailReason(reason)) return search(reason.error, depth + 1)
        if (Cause.isDieReason(reason)) return search(reason.defect, depth + 1)
        return undefined
      })
      .find((info) => info !== undefined)
  const code = codeOf(error)
  if (!code) return "cause" in error ? search(error.cause, depth + 1) : undefined
  return {
    code,
    reason:
      "message" in error && typeof error.message === "string" && error.message ? error.message : codes[code].reason,
  }
}

function codeOf(error: object): Code | undefined {
  // bun:sqlite reports the result code name, extended ones included (SQLITE_IOERR_WRITE).
  if ("code" in error && typeof error.code === "string" && error.code.startsWith("SQLITE_")) {
    const code = error.code
    return names.find((name) => code === name || code.startsWith(`${name}_`))
  }
  // node:sqlite reports the numeric code; its low byte is the primary result code.
  if ("code" in error && error.code === "ERR_SQLITE_ERROR" && "errcode" in error && typeof error.errcode === "number") {
    const primary = error.errcode & 0xff
    return names.find((name) => codes[name].primary === primary)
  }
  return undefined
}
