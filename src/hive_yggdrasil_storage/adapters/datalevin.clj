;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.adapters.datalevin
  "Yggdrasil adapter wrapping a Datalevin LMDB store — either a raw KV
   store (open-kv result) or a Datalog connection.

   Implements the yggdrasil.protocols SystemIdentity, Snapshotable, and
   Branchable on Datalevin's KV-level snapshot + WAL primitives. Snapshots
   are addressed by the WAL LSN that floors them: `snapshot-id` returns the
   stringified LSN and `snapshot-meta` joins it with `list-snapshots`
   metadata.

   Caller-facing behaviour:
   - Unsupported methods — `as-of` and all of Mergeable (`merge!`,
     `conflicts`, `diff`) — throw `ex-info` with
     `{:err :datalevin/unsupported-protocol-method ...}` rather than
     silently no-op'ing.
   - Branches are read-tip labels over a labelled-snapshot registry, NOT
     divergent history forks: `branch!` records `{branch-kw → snapshot-id}`
     and `checkout` switches the current branch. Mutations always land on
     `:main`.
   - The branch registry and current-branch are in-process atoms, NOT
     durable across JVM restarts.
   - `snapshot-id` is a stringified WAL LSN; it is not zero-padded, so
     consumers that sort must coerce to long."
  (:require [yggdrasil.protocols :as ygp]
            [datalevin.core :as dl]
            [taoensso.timbre :as log]))

(def ^:const datalevin-capabilities
  "Static capability advertisement for SystemIdentity — the yggdrasil
   protocols this adapter satisfies."
  #{:snapshotable :branchable})

(defn- unsupported!
  "Throw a structured error for protocol methods Datalevin doesn't model."
  [op]
  (throw (ex-info (str "Datalevin adapter does not implement yggdrasil " op)
                  {:err     :datalevin/unsupported-protocol-method
                   :backend :datalevin
                   :op      op})))

(defn- kv-handle
  "Extract the KV store from a handle. Accepts:
     - a raw KV store (open-kv result) — passed straight through
     - a Datalog connection atom — pulls `:store` from `(dl/db conn)`"
  [handle]
  (or (when (instance? clojure.lang.IDeref handle)
        (some-> handle dl/db :store))
      handle))

(defn- current-lsn
  "Read the current WAL committed-LSN watermark from the KV handle.
   Returns nil if the store wasn't opened with `:wal? true`."
  [handle]
  (try
    (some-> handle kv-handle dl/txlog-watermarks :committed-lsn)
    (catch Throwable t
      (log/debug t "txlog-watermarks unavailable — store likely opened without :wal?")
      nil)))

(defn- snapshot-entries
  "Return Datalevin's `list-snapshots` output coerced to a vector. nil-
   safe so callers don't have to guard."
  [handle]
  (or (try (vec (dl/list-snapshots (kv-handle handle))) (catch Throwable _ nil))
      []))

(defn- find-snapshot
  "Look up a snapshot entry whose `:lsn` matches `snap-id` (stringified
   long). Returns nil when not found."
  [handle snap-id]
  (let [target (try (Long/parseLong (str snap-id)) (catch Throwable _ nil))]
    (when target
      (some (fn [{:keys [lsn] :as e}]
              (when (= lsn target) e))
            (snapshot-entries handle)))))

;; ---------------------------------------------------------------------------
;; DatalevinSystem record — wraps either a raw KV store or a Datalog conn.
;; Branch-registry + current-branch are in-process atoms; the workspace
;; coordinator owns durable persistence (see ns docstring caveats).
;; ---------------------------------------------------------------------------

(defrecord DatalevinSystem [handle system-name branches-atom current-branch-atom]
  ygp/SystemIdentity
  (system-id    [_] system-name)
  (system-type  [_] :datalevin)
  (capabilities [_] datalevin-capabilities)

  ygp/Snapshotable
  (snapshot-id [_]
    (some-> (current-lsn handle) str))
  (parent-ids [_]
    ;; Snapshots in `list-snapshots` are LSN-ordered; "parents" of the
    ;; current state are all prior snapshot LSNs strictly < current LSN.
    (let [now (current-lsn handle)
          entries (snapshot-entries handle)]
      (->> entries
           (keep :lsn)
           (filter (fn [lsn] (and now (< lsn now))))
           (map str)
           set)))
  (as-of [_ _snap-id] (unsupported! 'as-of))
  (as-of [_ _snap-id _opts] (unsupported! 'as-of))
  (snapshot-meta [_ snap-id]
    (find-snapshot handle snap-id))
  (snapshot-meta [_ snap-id _opts]
    (find-snapshot handle snap-id))

  ygp/Branchable
  (branches [_]
    (-> @branches-atom keys set))
  (branches [_ _opts]
    (-> @branches-atom keys set))
  (current-branch [_]
    @current-branch-atom)
  (branch! [this name]
    (let [snap (some-> (current-lsn handle) str)]
      (swap! branches-atom assoc name snap)
      this))
  (branch! [this name from]
    (let [snap (cond
                 (keyword? from) (get @branches-atom from)
                 :else           (str from))]
      (swap! branches-atom assoc name snap)
      this))
  (branch! [this name from _opts]
    (ygp/branch! this name from))
  (delete-branch! [this name]
    (swap! branches-atom dissoc name)
    this)
  (delete-branch! [this name _opts]
    (ygp/delete-branch! this name))
  (checkout [this name]
    (when-not (contains? @branches-atom name)
      (throw (ex-info (str "Datalevin adapter: unknown branch " name)
                      {:err :datalevin/unknown-branch :branch name
                       :known (set (keys @branches-atom))})))
    (reset! current-branch-atom name)
    this)
  (checkout [this name _opts]
    (ygp/checkout this name))

  ;; --- Explicit unsupported surface (workspace coordinator MUST see ex-info,
  ;; not silent no-ops, when reaching for a protocol this backend can't honour)

  ygp/Mergeable
  (merge!    [_ _]   (unsupported! 'merge!))
  (conflicts [_ _ _] (unsupported! 'conflicts))
  (diff      [_ _ _] (unsupported! 'diff)))

(defn create-system
  "Wrap a Datalevin handle (raw KV store OR Datalog conn) into a
   DatalevinSystem satisfying yggdrasil Snapshotable + Branchable.

   `:handle`       — Datalevin KV store (open-kv result) or Datalog conn
                     atom (create-conn / get-conn result). The adapter
                     extracts the KV layer for snapshot/WAL ops.
   `:system-name`  — string id used by SystemIdentity/system-id.
   `:initial-branches` — optional map of {kw → snapshot-id}; defaults to
                     `{:main nil}` (snapshot-id resolves at first commit)."
  [{:keys [handle system-name initial-branches]
    :or   {initial-branches {:main nil}}}]
  (when-not (and handle system-name)
    (throw (ex-info "create-system requires :handle and :system-name"
                    {:provided (cond-> #{}
                                 handle      (conj :handle)
                                 system-name (conj :system-name))})))
  (->DatalevinSystem handle
                     system-name
                     (atom initial-branches)
                     (atom (or (-> initial-branches keys first) :main))))
