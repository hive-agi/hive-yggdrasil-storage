;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.fixture
  "Reproducible fixture runs over a git repository, through yggdrasil's git
   adapter.

   A run is a yggdrasil BRANCH forked from the fixture's baseline ref, living
   in its own worktree, so the fixture checkout itself is never touched:

     open!   fork `fx-<run>` at the baseline, answer its path and project-id
     drift   what the run changed since the baseline
     reset!  drop the branch and fork it again at the baseline
     close!  drop the branch and its worktree, then call the injected
             `:reap-fn` with the run's project-id so an index built over the
             run can be cleaned up by whoever owns it

   Each run gets a project-id of `<fixture>@<branch>`, so anything a caller
   indexes under it never collides with the fixture's own scope. Open runs
   are kept in a registry keyed by project-id, so a caller holding only that
   string can reset or close the run."
  (:refer-clojure :exclude [reset!])
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [malli.core :as m]
            [yggdrasil.adapters.git :as ygit]
            [yggdrasil.protocols :as ygp]))

(def Fixture
  "An open fixture run."
  [:map
   [:fixture :string]
   [:repo :string]
   [:baseline :string]
   [:baseline-sha :string]
   [:branch :string]
   [:path :string]
   [:project-id :string]
   [:system :some]])

(def OpenRequest
  [:map
   [:repo :string]
   [:fixture {:optional true} :string]
   [:baseline {:optional true} :string]
   [:run-id {:optional true} :string]
   [:root {:optional true} :string]])

(defonce ^:private runs
  (atom {}))

(defn- git!
  "Run git in `dir`; trimmed stdout, or throw with git's stderr."
  [dir & args]
  (let [{:keys [exit out err]} (apply shell/sh "git" "-C" dir args)]
    (if (zero? exit)
      (str/trim out)
      (throw (ex-info (str "git " (str/join " " args) ": " (str/trim err))
                      {:dir dir :args (vec args) :exit exit})))))

(defn default-root
  "Directory that holds fixture-run worktrees."
  []
  (str (io/file (System/getProperty "java.io.tmpdir") "hive-fixtures")))

(defn- fork!
  "Fork `branch` at `sha` as a new worktree of `system`; answer the checked-out
   system. Removes the empty `entries/` dir the git adapter creates for its own
   data ops, which is not part of any fixture."
  [system branch sha]
  (let [sys  (-> system (ygp/branch! branch sha) (ygp/checkout branch))
        extra (io/file (ygp/working-path sys) "entries")]
    (when (and (.isDirectory extra) (empty? (.list extra)))
      (.delete extra))
    sys))

(defn open!
  "Fork a run of the fixture at `repo` from its `:baseline` ref (default
   \"baseline\"). Answers the Fixture and registers it by project-id."
  [{:keys [repo fixture baseline run-id root]
    :or   {baseline "baseline"}}]
  (let [repo    (.getCanonicalPath (io/file repo))
        fixture (or fixture (.getName (io/file repo)))
        run-id  (or run-id (subs (str (random-uuid)) 0 8))
        branch  (str "fx-" run-id)
        sha     (git! repo "rev-parse" "--verify" (str baseline "^{commit}"))
        wt-dir  (str (io/file (or root (default-root)) fixture))
        system  (ygit/create repo {:worktrees-dir  wt-dir
                                   :initial-branch (git! repo "rev-parse" "--abbrev-ref" "HEAD")
                                   :system-name    (str "fixture:" fixture)})
        sys     (fork! system branch sha)
        fx      {:fixture      fixture
                 :repo         repo
                 :baseline     baseline
                 :baseline-sha sha
                 :branch       branch
                 :path         (ygp/working-path sys)
                 :project-id   (str fixture "@" branch)
                 :system       system}]
    (swap! runs assoc (:project-id fx) fx)
    fx))

(defn lookup
  "The open run registered under `project-id`, or nil."
  [project-id]
  (get @runs project-id))

(defn open-runs
  "Every open run, without its system handle."
  []
  (mapv #(dissoc % :system) (vals @runs)))

(defn drift
  "What the run has changed since its baseline: `:clean?`, the files that
   differ (committed, staged, unstaged or untracked), and the run's HEAD."
  [{:keys [path baseline-sha]}]
  (let [lines   (fn [s] (remove str/blank? (str/split-lines s)))
        tracked (lines (git! path "diff" "--name-only" baseline-sha))
        new     (lines (git! path "ls-files" "--others" "--exclude-standard"))
        head    (git! path "rev-parse" "HEAD")
        files   (vec (sort (distinct (concat tracked new))))]
    {:clean? (and (empty? files) (= head baseline-sha))
     :head   head
     :files  files}))

(defn reset!
  "Put the run back at its baseline: drop the branch and its worktree, then
   fork it again at the same baseline sha and path."
  [{:keys [system branch baseline-sha project-id] :as fx}]
  (ygp/delete-branch! system branch)
  (fork! system branch baseline-sha)
  (swap! runs assoc project-id fx)
  fx)

(defn close!
  "Drop the run's branch and worktree, unregister it, and call `:reap-fn`
   with its project-id. Answers {:closed? true :project-id .. :reaped ..}."
  ([fx] (close! fx {}))
  ([{:keys [system branch project-id]} {:keys [reap-fn]}]
   (ygp/delete-branch! system branch)
   (swap! runs dissoc project-id)
   (cond-> {:closed? true :project-id project-id}
     reap-fn (assoc :reaped (reap-fn project-id)))))

(m/=> open! [:=> [:cat OpenRequest] Fixture])
(m/=> drift [:=> [:cat Fixture] [:map [:clean? :boolean] [:head :string] [:files [:vector :string]]]])
