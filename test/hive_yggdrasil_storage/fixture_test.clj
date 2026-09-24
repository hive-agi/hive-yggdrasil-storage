;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.fixture-test
  "Open / drift / reset / close round-trip on a real temp git repository.

   git IS the port under test here, so these run real git in a temp dir
   rather than a double. The repo's HEAD is one commit PAST `baseline`, which
   is what proves a run starts at the baseline and not at whatever the
   fixture checkout happens to hold."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-yggdrasil-storage.fixture :as fx]))

(def ^:dynamic *repo* nil)
(def ^:dynamic *root* nil)

(defn- git [dir & args]
  (let [{:keys [exit out err]} (apply shell/sh "git" "-C" dir args)]
    (when-not (zero? exit) (throw (ex-info err {:args args})))
    (str/trim out)))

(defn- tmp-dir [prefix]
  (str (doto (io/file (System/getProperty "java.io.tmpdir")
                      (str prefix "-" (System/nanoTime)))
         (.mkdirs))))

(defn- delete-tree! [path]
  (let [f (io/file path)]
    (when (.exists f)
      (doseq [^java.io.File g (reverse (file-seq f))] (.delete g)))))

(defn- make-repo! []
  (let [repo (tmp-dir "acme-toy")]
    (git repo "init" "-q" "--initial-branch=main")
    (git repo "config" "user.email" "fixture@test")
    (git repo "config" "user.name" "Fixture")
    (git repo "config" "commit.gpgsign" "false")
    (git repo "config" "tag.gpgsign" "false")
    (spit (io/file repo "a.txt") "alpha\n")
    (spit (io/file repo "b.txt") "beta\n")
    (git repo "add" "-A")
    (git repo "commit" "-q" "-m" "baseline")
    (git repo "tag" "baseline")
    (spit (io/file repo "a.txt") "alpha after baseline\n")
    (git repo "commit" "-q" "-am" "past baseline")
    repo))

(use-fixtures :each
  (fn [t]
    (let [repo (make-repo!) root (tmp-dir "fixture-root")]
      (binding [*repo* repo *root* root]
        (try (t)
             (finally
               (doseq [r (fx/open-runs)] (fx/close! (fx/lookup (:project-id r))))
               (delete-tree! repo)
               (delete-tree! root)))))))

(defn- open [] (fx/open! {:repo *repo* :fixture "acme-toy" :run-id "t1" :root *root*}))

(deftest a-run-starts-at-the-baseline-not-at-head
  (let [run (open)]
    (is (= "acme-toy@fx-t1" (:project-id run)))
    (is (= "alpha\n" (slurp (io/file (:path run) "a.txt")))
        "the run holds the baseline content")
    (is (= "alpha after baseline\n" (slurp (io/file *repo* "a.txt")))
        "the fixture checkout is untouched")
    (is (= "main" (git *repo* "rev-parse" "--abbrev-ref" "HEAD")))
    (is (not (.exists (io/file (:path run) "entries")))
        "the adapter's own data dir does not leak into the fixture")
    (is (:clean? (fx/drift run)))
    (is (= run (fx/lookup (:project-id run))) "registered by project-id")))

(deftest drift-names-what-the-run-changed
  (let [run (open)]
    (spit (io/file (:path run) "b.txt") "beta edited\n")
    (spit (io/file (:path run) "c.txt") "new\n")
    (let [d (fx/drift run)]
      (is (false? (:clean? d)))
      (is (= ["b.txt" "c.txt"] (:files d))))))

(deftest reset-puts-the-run-back-at-the-baseline
  (let [run (open)]
    (spit (io/file (:path run) "b.txt") "beta edited\n")
    (spit (io/file (:path run) "c.txt") "new\n")
    (git (:path run) "add" "-A")
    (git (:path run) "-c" "commit.gpgsign=false" "commit" "-q" "-m" "work in the run")
    (is (false? (:clean? (fx/drift run))))
    (fx/reset! run)
    (is (= "beta\n" (slurp (io/file (:path run) "b.txt"))))
    (is (not (.exists (io/file (:path run) "c.txt"))))
    (is (:clean? (fx/drift run)) "back at the baseline sha, nothing dirty")))

(deftest close-removes-the-run-and-hands-its-project-id-to-the-reaper
  (let [run    (open)
        reaped (atom [])
        out    (fx/close! run {:reap-fn (fn [pid] (swap! reaped conj pid) {:retracted 3})})]
    (is (= {:closed? true :project-id "acme-toy@fx-t1" :reaped {:retracted 3}} out))
    (is (= ["acme-toy@fx-t1"] @reaped))
    (is (not (.exists (io/file (:path run)))) "the worktree is gone")
    (is (not (str/includes? (git *repo* "branch" "--list") "fx-t1")) "the branch is gone")
    (is (nil? (fx/lookup "acme-toy@fx-t1")))
    (is (empty? (fx/open-runs)))))

(deftest a-missing-baseline-is-an-error-not-a-run-at-head
  (is (thrown? clojure.lang.ExceptionInfo
               (fx/open! {:repo *repo* :baseline "no-such-tag" :root *root*})))
  (is (empty? (fx/open-runs))))
