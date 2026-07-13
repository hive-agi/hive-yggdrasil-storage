;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.adapters.proximum
  "Yggdrasil adapter extending the yggdrasil protocols onto Proximum's
   `proximum.hnsw.HnswIndex`, wrapped in a ProximumSystem record.

   Implements the yggdrasil.protocols SystemIdentity, Snapshotable,
   Branchable, Graphable, Committable, and GarbageCollectable over
   Proximum's versioning fns. Mergeable methods throw unsupported.

   Caller-facing behaviour:
   - Unsupported methods — Mergeable (`merge!`, `conflicts`, `diff`) —
     throw `ex-info` with
     `{:err :proximum/unsupported-protocol-method ...}` rather than
     silently no-op'ing.
   - `commit!` blocks the calling thread on the `sync!` channel via `<!!`;
     inside core.async go-blocks use `prox/sync!` directly with `<!`.
   - `gc-sweep!` called via the no-arg yggdrasil signature defaults the
     cutoff to `(java.util.Date.)`, which wipes everything older than NOW.
   - `as-of` needs the store-config; the wrapping record carries
     `:store-config` for it."
  (:require [yggdrasil.protocols :as ygp]
            [proximum.core :as prox]
            [clojure.core.async :as async]
            [taoensso.timbre :as log]))

(def ^:const proximum-capabilities
  "Static capability advertisement for the SystemIdentity protocol — the
   yggdrasil protocols this adapter satisfies."
  #{:snapshotable :branchable :graphable :committable :gc-collectable})

(defn- unsupported!
  "Throw a structured error for protocol methods Proximum doesn't model."
  [op]
  (throw (ex-info (str "Proximum does not implement yggdrasil " op)
                  {:err     :proximum/unsupported-protocol-method
                   :backend :proximum
                   :op      op})))

;; ---------------------------------------------------------------------------
;; Wrapping record — carries the system-name + store-config that Proximum's
;; top-level fns (load, load-commit) need but the index record doesn't hold.
;; ---------------------------------------------------------------------------

(defrecord ProximumSystem [index-atom system-name store-config]
  ygp/SystemIdentity
  (system-id   [_] system-name)
  (system-type [_] :proximum)
  (capabilities [_] proximum-capabilities)

  ygp/Snapshotable
  (snapshot-id  [_]    (prox/get-commit-id @index-atom))
  (parent-ids   [_]    (prox/parents @index-atom))
  (as-of [_ commit-id] (prox/load-commit store-config commit-id {:branch (prox/get-branch @index-atom)}))
  (snapshot-meta [_ snap-id] (prox/commit-info @index-atom snap-id))

  ygp/Branchable
  (branches        [_]      (prox/branches @index-atom))
  (current-branch  [_]      (prox/get-branch @index-atom))
  (branch!         [this n] (swap! index-atom prox/branch! n) this)
  (delete-branch!  [this n] (swap! index-atom prox/delete-branch! n) this)
  (checkout        [this n] (reset! index-atom (prox/load store-config {:branch n})) this)

  ygp/Graphable
  (history         [_]       (prox/history @index-atom))
  (ancestors       [_ sid]   (prox/ancestors @index-atom sid))
  (ancestor?       [_ a b]   (prox/ancestor? @index-atom a b))
  (common-ancestor [_ a b]   (prox/common-ancestor @index-atom a b))
  (commit-graph    [_]       (prox/commit-graph @index-atom))
  (commit-info     [_ sid]   (prox/commit-info @index-atom sid))

  ygp/Committable
  (commit! [_ msg]
    (let [ch (prox/sync! @index-atom msg)
          updated (async/<!! ch)]
      (when updated (reset! index-atom updated))
      (prox/get-commit-id @index-atom)))

  ygp/GarbageCollectable
  (gc-roots [_]
    (->> (prox/branches @index-atom)
         (mapv #(prox/get-commit-id (prox/load store-config {:branch %})))
         (filterv some?)))
  (gc-sweep! [_ _retain-snapshot-ids]
    (let [ch (prox/gc! @index-atom {} (java.util.Date.))]
      (async/<!! ch)))

  ygp/Mergeable
  (merge!    [_ _]   (unsupported! 'merge!))
  (conflicts [_ _ _] (unsupported! 'conflicts))
  (diff      [_ _ _] (unsupported! 'diff)))


(defn create-system
  "Wrap a Proximum index into a ProximumSystem that satisfies the
   yggdrasil protocols. `store-config` is the Proximum store-config map
   (`{:backend :file :path \"...\" :id #uuid \"...\"}`) used for
   load/load-commit. `system-name` identifies this system inside the
   yggdrasil workspace."
  [{:keys [index store-config system-name]}]
  (when-not (and index store-config system-name)
    (throw (ex-info "create-system requires :index, :store-config, :system-name"
                    {:provided (vec (filter (fn [[_ v]] (some? v))
                                            {:index index :store-config store-config :system-name system-name}))})))
  (->ProximumSystem (atom index) system-name store-config))
