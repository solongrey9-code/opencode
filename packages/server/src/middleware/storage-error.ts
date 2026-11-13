import { StorageFailure } from "@opencode/core/database/storage-failure"
import { StorageUnavailableError } from "@opencode/protocol/errors"
import { Effect } from "effect"
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http"

// Core treats every database failure as a defect, which would reach the client as a 500 with no body. When the
// database itself is the problem the client needs to know why, and that the request is safe to send again.
// Answered directly rather than declared on every endpoint: a declared 503 would make generated clients read
// every 503, including a starting server's and a proxy's, as this error.
export const storageErrorLayer = HttpRouter.middleware(
  (effect) =>
    effect.pipe(
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          const failure = StorageFailure.find(cause)
          if (!failure) return yield* Effect.failCause(cause)
          const request = yield* HttpServerRequest.HttpServerRequest
          yield* Effect.logError("request failed on storage", cause).pipe(
            Effect.annotateLogs({ route: `${request.method} ${request.url}`, code: failure.code }),
          )
          return HttpServerResponse.jsonUnsafe(
            {
              _tag: "StorageUnavailableError",
              message: StorageFailure.message(failure),
              reason: failure.reason,
            } satisfies typeof StorageUnavailableError.Encoded,
            { status: 503 },
          )
        }),
      ),
    ),
  { global: true },
)
