import { createEffect, createSignal, onCleanup, Show } from "solid-js"
import { useData } from "../context/data"
import { useTheme } from "../context/theme"

// A write that fails and the next one that succeeds can be milliseconds apart; only an outage that lasts this long shows.
const SHOW_AFTER_MS = 1_000

// Stays up for as long as the server can't write to its database, since anything sent meanwhile is not saved.
export function StorageBanner() {
  const data = useData()
  const theme = useTheme()
  const [reason, setReason] = createSignal<string>()

  createEffect(() => {
    const storage = data.server.storage()
    if (storage?.status !== "unavailable") return setReason(undefined)
    const timer = setTimeout(() => setReason(storage.reason ?? "the database is unavailable"), SHOW_AFTER_MS)
    onCleanup(() => clearTimeout(timer))
  })

  return (
    <Show when={reason()}>
      {(reason) => (
        <box flexShrink={0} paddingLeft={2} paddingRight={2} backgroundColor={theme.background.feedback.error.base}>
          <text fg={theme.text.feedback.error.base} wrapMode="word">
            Can't save to disk: {reason()}
          </text>
        </box>
      )}
    </Show>
  )
}
