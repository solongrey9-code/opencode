import { Show } from "solid-js"
import { useData } from "../context/data"
import { useTheme } from "../context/theme"

// Stays up for as long as the server can't write to its database, since anything sent meanwhile is not saved.
export function StorageBanner() {
  const data = useData()
  const theme = useTheme()
  const reason = () => {
    const storage = data.server.storage()
    if (storage?.status !== "unavailable") return undefined
    return storage.reason ?? "the database is unavailable"
  }

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
