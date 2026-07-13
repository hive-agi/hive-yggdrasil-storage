# hive-yggdrasil-storage

Yggdrasil-backed **versioned storage** for the hive ecosystem — snapshot /
branch / merge across heterogeneous backends under a shared Hybrid Logical
Clock.

Domain-agnostic and **hive-mcp-free**: it depends only on
[`yggdrasil`](https://github.com/replikativ/yggdrasil), the storage drivers,
and `hive-dsl`. Extracted from `hive-mcp` so the core no longer carries the
yggdrasil / datahike / datalevin coupling.

## Namespaces

| ns | role |
|----|------|
| `hive-yggdrasil-storage.adapters.datahike`  | `yggdrasil.protocols` adapter wrapping a Datahike conn (first-class versioning) |
| `hive-yggdrasil-storage.adapters.datalevin` | `yggdrasil.protocols` adapter wrapping a Datalevin store (snapshot as labelled tag) |
| `hive-yggdrasil-storage.adapters.proximum`  | `yggdrasil.protocols` adapter wrapping a Proximum vector store |
| `hive-yggdrasil-storage.workspace`          | late-bound facade over `yggdrasil.workspace` (degrades to nil + WARN if yggdrasil absent) |
| `hive-yggdrasil-storage.pilot`              | coordinated-commit + per-branch fan-out orchestration over the adapters |

## Usage

```clojure
(require '[hive-yggdrasil-storage.pilot :as pilot])

(pilot/register-adapters!
 {:memory {:kind :datahike  :handle conn  :system-name "hk-mem"}
  :carto  {:kind :datalevin :handle store :system-name "dl-carto"}})

(pilot/commit-on-wrap! {:session-id "smoke"})
```

The host owns the event bus and decides *when* to call `commit-on-wrap!` /
`branch-all-for-ling!` / `merge-all-for-ling!` from its own lifecycle events.

## License

MIT
