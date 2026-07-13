;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.pilot
  "Coordinated-commit + per-branch fan-out over the yggdrasil adapters.

   register-adapters! wraps storage handles and hands them to the workspace;
   commit-on-wrap! snapshots all registered systems under a shared HLC;
   branch-all-for-ling! / merge-all-for-ling! fan branch/merge across them.
   The host owns the event bus and decides when to call these. Ops degrade
   to :degraded/* keywords, never throw."
  (:require [hive-yggdrasil-storage.adapters.datahike :as adh]
            [hive-yggdrasil-storage.adapters.datalevin :as adl]
            [hive-yggdrasil-storage.workspace :as ws]
            [yggdrasil.protocols :as ygp]
            [taoensso.timbre :as log]
            [hive-yggdrasil-storage.schema :as schema]
            [malli.core :as m]))

(defonce ^:private registered-systems
  ^{:doc "Atom of {slot-key → ygp-protocol-satisfying-record}. Populated by
          `register-adapters!`, consumed by `commit-on-wrap!`."}
  (atom {}))

(def adapter-constructors
  "Adapter :kind -> constructor over an AdapterSpec. DIP/OCP swap point:
   a new backend registers a constructor here; build-adapter reads this
   map and needs no edit."
  {:datahike  (fn datahike-ctor  [{:keys [handle system-name]}]
                (adh/create-system {:conn handle :system-name system-name}))
   :datalevin (fn datalevin-ctor [{:keys [handle system-name]}]
                (adl/create-system {:handle handle :system-name system-name}))})

(defn registered
  "Return the current registry snapshot — read-only. Useful for
   diagnostics and for tests that want to assert on the wired set."
  []
  @registered-systems)

(defn buildable-kind
  "The adapter kind to build for `spec`, or nil when `spec` is not
   buildable (unknown :kind, or missing :handle / blank :system-name). Pure."
  [{:keys [kind handle system-name]}]
  (when (and (some? handle)
             (string? system-name) (pos? (count system-name))
             (contains? schema/adapter-kinds kind))
    kind))

(defn build-adapter
  "Wrap a slot's storage handle in the matching yggdrasil adapter, looking
   the constructor up in `constructors` (default adapter-constructors) by
   the spec's buildable-kind. Returns the adapter, or nil + WARN when the
   spec is not buildable / its kind has no registered constructor."
  ([slot-key spec] (build-adapter adapter-constructors slot-key spec))
  ([constructors slot-key spec]
   (if-let [ctor (some->> spec buildable-kind (get constructors))]
     (ctor spec)
     (do (log/warn "pilot: unbuildable adapter spec — slot skipped"
                   {:slot slot-key :spec spec})
         nil))))

(defn register-adapters!
  "Wrap each spec in `specs` (a SlotSpecs map) in its adapter, hand it to
   the workspace, and stash it locally. Idempotent — re-registering a slot
   replaces its adapter. Specs failing schema/valid-adapter-spec? are
   logged and skipped (best-effort). Returns the resulting registry map."
  [specs]
  (doseq [[slot-key spec] specs]
    (when-not (schema/valid-adapter-spec? spec)
      (log/warn "pilot: adapter spec failed validation" {:slot slot-key :spec spec}))
    (when-let [adapter (build-adapter slot-key spec)]
      (ws/manage-system! adapter)
      (swap! registered-systems assoc slot-key adapter)
      (log/info "pilot: registered adapter"
                {:slot slot-key
                 :system-id (ygp/system-id adapter)
                 :system-type (ygp/system-type adapter)})))
  @registered-systems)

(defn unregister-adapters!
  "Drop every registered adapter from the workspace + local registry.
   Used at shutdown and in test teardown."
  []
  (let [snapshot @registered-systems]
    (doseq [adapter (vals snapshot)]
      (ws/unmanage-system! (ygp/system-id adapter)))
    (reset! registered-systems {})
    snapshot))

(defn- commit-fn-for
  "Commit-fn delegating to `adapter`: returns its snapshot-id, or nil on
   failure. The returned fn ignores its system arg."
  [adapter]
  (fn [_system]
    (try
      (ygp/snapshot-id adapter)
      (catch Throwable t
        (log/warn t "pilot: snapshot-id threw" {:system-id (ygp/system-id adapter)})
        nil))))

(defn build-commit-fn-map
  "Assemble the coordinated-commit map {system-id -> commit-fn} from
   `entries` (a CommitEntries seq of {:system-id :commit-fn}). Distinct
   :system-id keys, last wins. Pure."
  [entries]
  (into {} (map (juxt :system-id :commit-fn)) entries))

(defn commit-on-wrap!
  "Snapshot every registered system under a shared HLC. `metadata` is
   forwarded for log context only. Returns the coordinated-commit record,
   or a DegradedMode keyword:
     :degraded/no-workspace            workspace not started
     :degraded/no-adapters             registry empty
     :degraded/coordinated-commit-nil  workspace up, commit returned nil"
  [metadata]
  (let [systems @registered-systems]
    (cond
      (not (ws/started?))
      (do (log/debug "pilot: workspace not started — commit skipped" {:metadata metadata})
          :degraded/no-workspace)

      (empty? systems)
      (do (log/warn "pilot: no adapters registered — commit skipped" {:metadata metadata})
          :degraded/no-adapters)

      :else
      (let [entries       (for [[_slot adapter] systems]
                            {:system-id (ygp/system-id adapter)
                             :commit-fn (commit-fn-for adapter)})
            commit-fn-map (build-commit-fn-map entries)
            result        (ws/coordinated-commit! commit-fn-map)]
        (if (some? result)
          (do (log/info "pilot: coordinated-commit OK"
                        {:metadata metadata :systems (vec (keys commit-fn-map))})
              result)
          (do (log/warn "pilot: coordinated-commit returned nil" {:metadata metadata})
              :degraded/coordinated-commit-nil))))))

;; Per-ling fork/merge = workspace branch!/merge! fan-out.

(defn ling-branch-name
  "Workspace branch keyword for `ling-id`: :ling/<id>, or nil when
   `ling-id` is nil / empty. Pure."
  [ling-id]
  (when-let [id (some-> ling-id str not-empty)]
    (keyword "ling" id)))

(defn ok-branch-result
  "Success BranchResult for a branch fan-out entry. Pure."
  [{:keys [slot system-id branch]}]
  {:slot slot :system-id system-id :branch branch :ok? true})

(defn- safe-branch!
  "Call ygp/branch! on `adapter`, swallowing per-adapter failures so one
   backend can't poison the fan-out. Returns a BranchResult."
  [slot-key adapter branch-kw]
  (let [system-id (ygp/system-id adapter)]
    (try
      (ygp/branch! adapter branch-kw)
      (ok-branch-result {:slot slot-key :system-id system-id :branch branch-kw})
      (catch Throwable t
        (log/warn t "pilot: branch! failed — fan-out continues"
                  {:slot slot-key :system-id system-id :branch branch-kw})
        {:slot slot-key :system-id system-id :branch branch-kw
         :ok? false :error (.getMessage t)}))))

(defn unsupported-merge-result
  "MergeResult for an adapter that lacks the :mergeable capability. Pure."
  [{:keys [slot system-id branch]}]
  {:slot slot :system-id system-id :branch branch :merged? false :reason "unsupported"})

(defn- safe-merge!
  "Call ygp/merge! on `adapter` when it advertises :mergeable. Returns a
   MergeResult; adapters without :mergeable record :merged? false with
   :reason \"unsupported\"."
  [slot-key adapter branch-kw]
  (let [system-id (ygp/system-id adapter)
        caps      (try (ygp/capabilities adapter) (catch Throwable _ #{}))]
    (if-not (contains? caps :mergeable)
      (unsupported-merge-result {:slot slot-key :system-id system-id :branch branch-kw})
      (try
        (ygp/merge! adapter branch-kw)
        {:slot slot-key :system-id system-id :branch branch-kw :merged? true}
        (catch Throwable t
          (log/warn t "pilot: merge! failed — fan-out continues"
                    {:slot slot-key :system-id system-id :branch branch-kw})
          {:slot slot-key :system-id system-id :branch branch-kw
           :merged? false :error (.getMessage t)})))))

(defn branch-all-for-ling!
  "Fan out `branch!` across every registered adapter for `ling-id`.
   Returns a vector of per-system result maps. Returns
   `:degraded/no-adapters` when nothing is registered — caller decides
   whether that's fatal or expected."
  [ling-id]
  (let [systems @registered-systems
        branch  (ling-branch-name ling-id)]
    (cond
      (nil? branch)
      (do (log/warn "pilot: branch-all-for-ling! given empty ling-id" {:ling-id ling-id})
          :degraded/empty-ling-id)

      (empty? systems)
      (do (log/debug "pilot: branch-all-for-ling! — no adapters" {:ling-id ling-id})
          :degraded/no-adapters)

      :else
      (let [results (mapv (fn [[slot adapter]] (safe-branch! slot adapter branch))
                          systems)]
        (log/info "pilot: branched workspace for ling"
                  {:ling-id ling-id :branch branch :results results})
        results))))

(defn merge-all-for-ling!
  "Fan out `merge!` back to :main across every registered adapter for
   `ling-id`. Adapters lacking :mergeable record `:merged? false
   :reason \"unsupported\"` and the branch lingers as a labelled
   snapshot. Returns vector of per-system result maps, or a
   `:degraded/*` keyword."
  [ling-id]
  (let [systems @registered-systems
        branch  (ling-branch-name ling-id)]
    (cond
      (nil? branch)        :degraded/empty-ling-id
      (empty? systems)     :degraded/no-adapters
      :else
      (let [results (mapv (fn [[slot adapter]] (safe-merge! slot adapter branch))
                          systems)]
        (log/info "pilot: merged ling branch back to :main"
                  {:ling-id ling-id :branch branch :results results})
        results))))

;; Contract spine — the schema ns is the single source for both these m/=>
;; function schemas and the schema-driven tests (see pilot-test).
(do
  (m/=> ling-branch-name       [:=> [:cat [:maybe :string]]       [:maybe schema/LingBranch]])
  (m/=> buildable-kind         [:=> [:cat schema/RawAdapterSpec]  [:maybe schema/AdapterKind]])
  (m/=> build-commit-fn-map    [:=> [:cat schema/CommitEntries]   schema/CommitFnMap])
  (m/=> ok-branch-result       [:=> [:cat schema/BranchInput]     schema/BranchResult])
  (m/=> unsupported-merge-result [:=> [:cat schema/BranchInput]   schema/MergeResult]))