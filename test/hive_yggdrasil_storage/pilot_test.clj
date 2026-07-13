;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.pilot-test
  "Schema-driven free coverage for the pure pilot kernels + plain deftests
   for the classification / DIP / degraded contracts a schema cannot state."
  (:require [clojure.test :refer [deftest testing is are]]
            [hive-schemas.test :as hst]
            [hive-yggdrasil-storage.pilot :as pilot]
            [hive-yggdrasil-storage.schema :as schema]))

;; =============================================================================
;; Schema-driven facets — a malli schema drives property + mutation with NO
;; hand-written generator, oracle, or mutant (hive-schemas.test).
;; =============================================================================

;; ling-branch-name — scalar output (keyword | nil): conformance + relation.
;; Scalar output has no map-entries to corrupt -> :mutation false.
(hst/deftrifecta-from-schema ling-branch pilot/ling-branch-name
  {:in  [:maybe :string]
   :out [:maybe schema/LingBranch]
   :rel (fn [in out]
          (= out (when-let [id (some-> in str not-empty)]
                   (keyword "ling" id))))
   :mutation false
   :num-tests 200})

;; build-adapter validation path — pure kind resolution over a permissive
;; boundary spec. Scalar output (kind | nil) -> :mutation false.
(hst/deftrifecta-from-schema buildable-kind pilot/buildable-kind
  {:in  schema/RawAdapterSpec
   :out [:maybe schema/AdapterKind]
   :rel (fn [spec out]
          (= out (let [{:keys [kind handle system-name]} spec]
                   (when (and (some? handle)
                              (string? system-name) (pos? (count system-name))
                              (contains? #{:datahike :datalevin} kind))
                     kind))))
   :mutation false
   :num-tests 200})

;; commit-fn-map construction — dynamic map-of output: keyset invariant.
;; No fixed required keys to corrupt -> :mutation false.
(hst/deftrifecta-from-schema commit-fn-map pilot/build-commit-fn-map
  {:in  schema/CommitEntries
   :out schema/CommitFnMap
   :rel (fn [entries out]
          (= (set (keys out)) (set (map :system-id entries))))
   :mutation false
   :num-tests 200})

;; BranchResult / MergeResult value objects — fixed-key maps: the schema
;; auto-derives drop-key + wrong-type MUTANTS (mutation facet, zero
;; hand-written mutants), killed by the output oracle.
(hst/deftrifecta-from-schema ok-branch pilot/ok-branch-result
  {:in  schema/BranchInput
   :out schema/BranchResult
   :rel (fn [in out]
          (= out {:slot (:slot in) :system-id (:system-id in)
                  :branch (:branch in) :ok? true}))
   :num-tests 200})

(hst/deftrifecta-from-schema unsupported-merge pilot/unsupported-merge-result
  {:in  schema/BranchInput
   :out schema/MergeResult
   :rel (fn [in out]
          (= out {:slot (:slot in) :system-id (:system-id in)
                  :branch (:branch in) :merged? false :reason "unsupported"}))
   :num-tests 200})

;; =============================================================================
;; Plain deftests — the classification table, DIP/OCP swap point, and the
;; degraded-mode enum: contracts a schema cannot express.
;; =============================================================================

(deftest ling-branch-name-table
  (are [in out] (= out (pilot/ling-branch-name in))
    nil   nil
    ""    nil
    "abc" :ling/abc
    "7"   (keyword "ling" "7")))

(deftest buildable-kind-classification
  (testing "known kind + full spec -> kind; anything else -> nil"
    (are [spec expected] (= expected (pilot/buildable-kind spec))
      {:kind :datahike  :handle :h :system-name "s"} :datahike
      {:kind :datalevin :handle :h :system-name "s"} :datalevin
      {:kind :proximum  :handle :h :system-name "s"} nil
      {:kind :datahike  :handle nil :system-name "s"} nil
      {:kind :datahike  :handle :h :system-name ""}  nil
      {:kind :datahike  :handle :h :system-name nil} nil
      {:handle :h :system-name "s"}                  nil)))

(deftest build-adapter-dip-injection
  (testing "the constructor registry is the DIP swap point — no real store"
    (let [ctors {:datahike  (fn [spec] [:built :datahike (:system-name spec)])
                 :datalevin (fn [spec] [:built :datalevin (:system-name spec)])}]
      (is (= [:built :datahike "S"]
             (pilot/build-adapter ctors :slot {:kind :datahike :handle :h :system-name "S"}))
          "valid spec dispatches to the injected constructor")
      (is (nil? (pilot/build-adapter ctors :slot {:kind :unknown :handle :h :system-name "S"}))
          "unknown kind -> nil")
      (is (nil? (pilot/build-adapter ctors :slot {:kind :datahike :handle nil :system-name "S"}))
          "missing handle -> nil")
      (is (nil? (pilot/build-adapter ctors :slot {:kind :datahike :handle :h :system-name ""}))
          "blank system-name -> nil")
      (is (nil? (pilot/build-adapter {:datahike (:datahike ctors)} :slot
                                     {:kind :datalevin :handle :h :system-name "S"}))
          "kind with no registered constructor -> nil"))))

(deftest build-commit-fn-map-keys
  (let [f       (fn [_] :snap)
        entries [{:system-id "a" :commit-fn f}
                 {:system-id "b" :commit-fn f}
                 {:system-id "a" :commit-fn f}]
        m       (pilot/build-commit-fn-map entries)]
    (is (= #{"a" "b"} (set (keys m))) "distinct system-ids, dedup")
    (is (every? fn? (vals m)))
    (is (= {} (pilot/build-commit-fn-map [])))))

(deftest degraded-mode-membership
  (testing "every :degraded/* the pilot returns is a DegradedMode"
    (are [k] (schema/degraded-mode? k)
      :degraded/no-workspace
      :degraded/no-adapters
      :degraded/coordinated-commit-nil
      :degraded/empty-ling-id))
  (testing "rejects non-members"
    (is (not (schema/degraded-mode? :degraded/other)))
    (is (not (schema/degraded-mode? :ok)))))
