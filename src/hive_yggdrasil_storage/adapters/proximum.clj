;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.adapters.proximum
  "Yggdrasil adapter extending current yggdrasil protocols onto a Proximum
   index wrapped in ProximumSystem.

   Proximum's public API supports snapshot loading, branches, commit graph,
   and async sync. Exact snapshot-set GC and generic merge are not public
   operations, so their required yggdrasil protocol methods fail explicitly
   and those capabilities are not advertised.

   Candidate overlays remain a host-supplied projection seam. Proximum has no
   reversible multi-system transaction primitive; implementing
   ICandidateOverlaySystem here would invent global atomicity. Candidate
   lifecycle may still use Proximum as proposal-only semantic projection via
   its generic IProjectionBuilder role.

   commit! blocks on sync!'s channel via <!!; callers already inside go blocks
   should call proximum.core/sync! directly. store-config is retained for
   load and load-commit operations."
  (:require [clojure.core.async :as async]
            [proximum.core :as prox]
            [yggdrasil.protocols :as ygp]))

(def ^:const proximum-capabilities
  "Capabilities Proximum can honor through its current public API. Exact
   snapshot deletion and generic merge are deliberately not advertised."
  #{:snapshotable :branchable :graphable :committable})

(def candidate-overlay-seam
  "Machine-readable statement of the honest candidate integration boundary.
   A host supplies isolated projection/overlay behavior; this adapter provides
   proposal-only semantic storage and claims no cross-system atomicity."
  {:candidate-overlay/status :host-supplied
   :candidate-overlay/projection-role :semantic
   :candidate-overlay/authority :proposal-only
   :candidate-overlay/reason :proximum/no-reversible-multi-system-transaction})

(defn- unsupported!
  "Throw a structured error for protocol methods Proximum doesn't model."
  [op]
  (throw (ex-info (str "Proximum does not implement yggdrasil " op)
                  {:err     :proximum/unsupported-protocol-method
                   :backend :proximum
                   :op      op})))

(defn- with-index
  "Return a fresh adapter value over `index`; never mutate prior system values."
  [system index]
  (assoc system :index-atom (atom index)))

;; ---------------------------------------------------------------------------
;; Wrapping record — carries the system-name + store-config that Proximum's
;; top-level fns (load, load-commit) need but the index record doesn't hold.
;; ---------------------------------------------------------------------------

(defrecord ProximumSystem [index-atom system-name store-config]
  ygp/SystemIdentity
  (system-id [_] system-name)
  (system-type [_] :proximum)
  (capabilities [_] proximum-capabilities)

  ygp/Snapshotable
  (snapshot-id [_]
    (prox/get-commit-id @index-atom))
  (parent-ids [_]
    (or (prox/parents @index-atom) #{}))
  (as-of [this commit-id]
    (ygp/as-of this commit-id nil))
  (as-of [_ commit-id opts]
    (prox/load-commit
     store-config commit-id
     (merge {:branch (prox/get-branch @index-atom)}
            (select-keys (or opts {})
                         [:branch :cache-size :mmap-dir :mmap-path :store]))))
  (snapshot-meta [this snapshot-id]
    (ygp/snapshot-meta this snapshot-id nil))
  (snapshot-meta [_ snapshot-id _opts]
    (prox/commit-info @index-atom snapshot-id))

  ygp/Branchable
  (branches [this]
    (ygp/branches this nil))
  (branches [_ _opts]
    (or (prox/branches @index-atom) #{}))
  (current-branch [_]
    (prox/get-branch @index-atom))
  (branch! [this name]
    (ygp/branch! this name nil nil))
  (branch! [this name from]
    (ygp/branch! this name from nil))
  (branch! [this name from opts]
    (let [load-opts (select-keys (or opts {})
                                 [:cache-size :mmap-dir :mmap-path :store])
          source-index
          (cond
            (nil? from) @index-atom
            (keyword? from)
            (prox/load store-config (assoc load-opts :branch from))
            :else
            (prox/load-commit
             store-config from
             (assoc load-opts :branch (prox/get-branch @index-atom))))]
      (prox/branch! source-index name)
      (with-index this @index-atom)))
  (delete-branch! [this name]
    (ygp/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (with-index this (prox/delete-branch! @index-atom name)))
  (checkout [this name]
    (ygp/checkout this name nil))
  (checkout [this name opts]
    (with-index
      this
      (prox/load
       store-config
       (merge {:branch name}
              (select-keys (or opts {})
                           [:cache-size :mmap-dir :mmap-path :store])))))

  ygp/Graphable
  (history [this]
    (ygp/history this nil))
  (history [_ _opts]
    (prox/history @index-atom))
  (ancestors [this snapshot-id]
    (ygp/ancestors this snapshot-id nil))
  (ancestors [_ snapshot-id _opts]
    (prox/ancestors @index-atom snapshot-id))
  (ancestor? [this a b]
    (ygp/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (prox/ancestor? @index-atom a b))
  (common-ancestor [this a b]
    (ygp/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (prox/common-ancestor @index-atom a b))
  (commit-graph [this]
    (ygp/commit-graph this nil))
  (commit-graph [_ _opts]
    (prox/commit-graph @index-atom))
  (commit-info [this snapshot-id]
    (ygp/commit-info this snapshot-id nil))
  (commit-info [_ snapshot-id _opts]
    (prox/commit-info @index-atom snapshot-id))

  ygp/Committable
  (commit! [this]
    (ygp/commit! this nil {}))
  (commit! [this message]
    (ygp/commit! this message {}))
  (commit! [this message opts]
    (let [sync-opts (cond-> (or opts {})
                      message (assoc :message message))
          updated (async/<!! (prox/sync! @index-atom sync-opts))]
      (when-not updated
        (throw (ex-info "Proximum sync! returned no updated index"
                        {:err :proximum/sync-empty})))
      (with-index this updated)))

  ygp/GarbageCollectable
  (gc-roots [_]
    (->> (prox/commit-graph @index-atom)
         :branches
         vals
         (filter some?)
         set))
  (gc-sweep! [_ _snapshot-ids]
    (unsupported! 'gc-sweep!))
  (gc-sweep! [_ _snapshot-ids _opts]
    (unsupported! 'gc-sweep!))

  ygp/Mergeable
  (merge! [_ _source]
    (unsupported! 'merge!))
  (merge! [_ _source _opts]
    (unsupported! 'merge!))
  (conflicts [_ _a _b]
    (unsupported! 'conflicts))
  (conflicts [_ _a _b _opts]
    (unsupported! 'conflicts))
  (diff [_ _a _b]
    (unsupported! 'diff))
  (diff [_ _a _b _opts]
    (unsupported! 'diff)))


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
