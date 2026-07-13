;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.schema
  "Malli value-objects for the yggdrasil-storage coordination seams.

   Each schema is a plain malli form, usable both as an m/=> contract and
   as a hive-schemas.test :in/:out driver.

     AdapterKind     :datahike | :datalevin
     NonBlankString  string, length >= 1
     LingBranch      qualified keyword in the \"ling\" namespace
     RawAdapterSpec  permissive host-boundary spec
     AdapterSpec     validated spec {:kind :handle :system-name}
     SlotSpecs       {slot-key AdapterSpec}
     BranchInput     input to ok-branch-result / unsupported-merge-result
     BranchResult    per-adapter branch! fan-out result
     MergeResult     per-adapter merge! fan-out result
     DegradedMode    the :degraded/* keywords the pilot returns
     CommitEntry     {:system-id :commit-fn}
     CommitEntries   [CommitEntry]
     CommitFnMap     {system-id commit-fn}"
  (:require [malli.core :as m]))

(def AdapterKind [:enum :datahike :datalevin])

(def adapter-kinds
  "The AdapterKind enum as a set — the dispatch domain for build-adapter."
  #{:datahike :datalevin})

(def NonBlankString [:string {:min 1}])

(def LingBranch
  [:and :qualified-keyword [:fn (fn ling-namespaced? [k] (= "ling" (namespace k)))]])

(def RawAdapterSpec
  [:map
   [:kind :keyword]
   [:handle :any]
   [:system-name [:maybe :string]]])

(def AdapterSpec
  [:map {:closed false}
   [:kind AdapterKind]
   [:handle :any]
   [:system-name NonBlankString]])

(def SlotSpecs [:map-of :keyword AdapterSpec])

(def BranchInput
  [:map
   [:slot :keyword]
   [:system-id NonBlankString]
   [:branch :keyword]])

(def BranchResult
  [:map {:closed false}
   [:slot :keyword]
   [:system-id NonBlankString]
   [:branch :keyword]
   [:ok? :boolean]
   [:error {:optional true} [:maybe :string]]])

(def MergeResult
  [:map {:closed false}
   [:slot :keyword]
   [:system-id NonBlankString]
   [:branch :keyword]
   [:merged? :boolean]
   [:reason {:optional true} :string]
   [:error {:optional true} [:maybe :string]]])

(def DegradedMode
  [:enum :degraded/no-workspace :degraded/no-adapters
   :degraded/coordinated-commit-nil :degraded/empty-ling-id])

(def CommitEntry
  [:map
   [:system-id NonBlankString]
   [:commit-fn :any]])

(def CommitEntries [:sequential CommitEntry])

(def CommitFnMap [:map-of NonBlankString :any])

(def registry
  "Keyword -> value-object schema, for callers that prefer registry refs."
  {::adapter-kind     AdapterKind
   ::non-blank-string NonBlankString
   ::ling-branch      LingBranch
   ::raw-adapter-spec RawAdapterSpec
   ::adapter-spec     AdapterSpec
   ::slot-specs       SlotSpecs
   ::branch-input     BranchInput
   ::branch-result    BranchResult
   ::merge-result     MergeResult
   ::degraded-mode    DegradedMode
   ::commit-entry     CommitEntry
   ::commit-entries   CommitEntries
   ::commit-fn-map    CommitFnMap})

(defn valid-adapter-spec?
  "True when `spec` conforms to AdapterSpec."
  [spec]
  (m/validate AdapterSpec spec))

(defn degraded-mode?
  "True when `x` is one of the pilot's :degraded/* keywords."
  [x]
  (m/validate DegradedMode x))
