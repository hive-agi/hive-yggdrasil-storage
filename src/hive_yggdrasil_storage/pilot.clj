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
            [taoensso.timbre :as log]))

(defonce ^:private registered-systems
  ^{:doc "Atom of {slot-key → ygp-protocol-satisfying-record}. Populated by
          `register-adapters!`, consumed by `commit-on-wrap!`."}
  (atom {}))

(defn registered
  "Return the current registry snapshot — read-only. Useful for
   diagnostics and for tests that want to assert on the wired set."
  []
  @registered-systems)

(defn- build-adapter
  "Wrap a slot's storage handle in the matching yggdrasil adapter.
   Returns nil + WARN on unknown `:kind` so registration stays best-effort."
  [slot-key {:keys [kind handle system-name] :as spec}]
  (when (and handle system-name)
    (case kind
      :datahike  (adh/create-system {:conn   handle
                                     :system-name system-name})
      :datalevin (adl/create-system {:handle handle
                                     :system-name system-name})
      (do (log/warn "pilot: unknown adapter kind for slot"
                    {:slot slot-key :spec spec})
          nil))))

(defn register-adapters!
  "Wrap each `slot-spec` in the matching adapter, hand it to the
   workspace coordinator, and stash it locally so `commit-on-wrap!`
   can build the commit-fn-map. Idempotent — re-registering a slot
   replaces the previous adapter (and re-runs `manage-system!`).

   `specs` ::= `{slot-key {:kind :datahike|:datalevin
                           :handle <conn-or-store>
                           :system-name string}}`

   Returns the resulting registry map."
  [specs]
  (doseq [[slot-key spec] specs
          :let [adapter (build-adapter slot-key spec)]
          :when adapter]
    (ws/manage-system! adapter)
    (swap! registered-systems assoc slot-key adapter)
    (log/info "pilot: registered adapter"
              {:slot slot-key
               :system-id (ygp/system-id adapter)
               :system-type (ygp/system-type adapter)}))
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
  "Default commit-fn delegated to the adapter: snapshot the current state
   and return the snapshot-id. Adapters that don't return a stable id
   from `snapshot-id` will fall back to the LSN/UUID their backend
   exposes — both are coordinator-safe."
  [adapter]
  (fn [_system]
    (try
      (ygp/snapshot-id adapter)
      (catch Throwable t
        (log/warn t "pilot: snapshot-id threw"
                  {:system-id (ygp/system-id adapter)})
        nil))))

(defn commit-on-wrap!
  "Build the commit-fn-map from the current registry and invoke
   `workspace/coordinated-commit!`. Returns the coordinated-commit
   record, or one of:

     :degraded/no-workspace   — workspace not started (yggdrasil missing
                                or `start-workspace!` never called)
     :degraded/no-adapters    — registry empty (host didn't wire)
     :degraded/coordinated-commit-nil — workspace up but
                                yggdrasil/coordinated-commit! returned nil

   `metadata` is forwarded for log context (session-id, agent-id, etc.) —
   the actual commit message is owned by yggdrasil's HLC."
  [metadata]
  (let [systems @registered-systems]
    (cond
      (not (ws/started?))
      (do (log/debug "pilot: workspace not started — coordinated-commit skipped"
                     {:metadata metadata})
          :degraded/no-workspace)

      (empty? systems)
      (do (log/warn "pilot: no adapters registered — coordinated-commit skipped"
                    {:metadata metadata})
          :degraded/no-adapters)

      :else
      (let [commit-fn-map (into {}
                                (for [[_slot adapter] systems]
                                  [(ygp/system-id adapter) (commit-fn-for adapter)]))
            result        (ws/coordinated-commit! commit-fn-map)]
        (if (some? result)
          (do (log/info "pilot: coordinated-commit OK"
                        {:metadata metadata
                         :systems (vec (keys commit-fn-map))})
              result)
          (do (log/warn "pilot: workspace coordinated-commit returned nil"
                        {:metadata metadata})
              :degraded/coordinated-commit-nil))))))

;; ---------------------------------------------------------------------------
;; Fork = workspace branch! ; merge-back = workspace merge!
;;
;; Forking an isolated actor forks the workspace: every adapter under
;; coordination gets a per-actor branch so the actor's writes land on an
;; isolated commit graph. Merge folds that branch back to :main where the
;; adapter's Mergeable surface allows; backends that lack merge semantics
;; keep the branch as a labelled snapshot and skip the merge phase.
;; ---------------------------------------------------------------------------

(defn ling-branch-name
  "Derive the workspace branch keyword for `ling-id`. Pure — no side
   effects. Always returns a namespaced keyword so dispatch is cheap
   and the registry stays self-describing."
  [ling-id]
  (when-let [id (some-> ling-id str not-empty)]
    (keyword "ling" id)))

(defn- safe-branch!
  "Call ygp/branch! on `adapter`, swallowing per-adapter failures so one
   misbehaving backend can't poison the fan-out. Returns
   `{:slot kw :system-id str :branch kw :ok? bool :error str?}`."
  [slot-key adapter branch-kw]
  (try
    (ygp/branch! adapter branch-kw)
    {:slot      slot-key
     :system-id (ygp/system-id adapter)
     :branch    branch-kw
     :ok?       true}
    (catch Throwable t
      (log/warn t "pilot: branch! failed for slot — fan-out continues"
                {:slot slot-key
                 :system-id (ygp/system-id adapter)
                 :branch branch-kw})
      {:slot      slot-key
       :system-id (ygp/system-id adapter)
       :branch    branch-kw
       :ok?       false
       :error     (.getMessage t)})))

(defn- safe-merge!
  "Call ygp/merge! on `adapter` if it advertises :mergeable. Returns
   `{:slot :system-id :branch :merged? :error?}`. Adapters without
   Mergeable record `:merged? false` with a `:reason \"unsupported\"`."
  [slot-key adapter branch-kw]
  (let [caps (try (ygp/capabilities adapter) (catch Throwable _ #{}))]
    (if-not (contains? caps :mergeable)
      {:slot      slot-key
       :system-id (ygp/system-id adapter)
       :branch    branch-kw
       :merged?   false
       :reason    "unsupported"}
      (try
        (ygp/merge! adapter branch-kw)
        {:slot      slot-key
         :system-id (ygp/system-id adapter)
         :branch    branch-kw
         :merged?   true}
        (catch Throwable t
          (log/warn t "pilot: merge! failed for slot — fan-out continues"
                    {:slot slot-key
                     :system-id (ygp/system-id adapter)
                     :branch branch-kw})
          {:slot      slot-key
           :system-id (ygp/system-id adapter)
           :branch    branch-kw
           :merged?   false
           :error     (.getMessage t)})))))

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
