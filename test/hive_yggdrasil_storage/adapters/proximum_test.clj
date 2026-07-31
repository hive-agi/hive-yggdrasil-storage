;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-yggdrasil-storage.adapters.proximum-test
  "Contract tests for Proximum's current yggdrasil protocol adapter."
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [hive-yggdrasil-storage.adapters.proximum :as adapter]
            [hive-yggdrasil-storage.pilot :as pilot]
            [hive-yggdrasil-storage.schema :as schema]
            [proximum.core :as prox]
            [yggdrasil.protocols :as ygp]))

(def ^:private store-config
  {:backend :memory :id :proximum-test})

(defn- make-system []
  (adapter/create-system
   {:index {:commit-id :c0 :branch :main}
    :store-config store-config
    :system-name "semantic"}))

(defn- channel-of [value]
  (let [ch (async/chan 1)]
    (when (some? value)
      (async/>!! ch value))
    (async/close! ch)
    ch))

(deftest schema-and-default-pilot-dispatch
  (let [spec {:kind :proximum
              :handle :index
              :system-name "semantic"
              :store-config store-config}
        captured (atom nil)]
    (is (schema/valid-adapter-spec? spec))
    (is (= :proximum (pilot/buildable-kind spec)))
    (is (nil? (pilot/buildable-kind (dissoc spec :store-config))))
    (with-redefs [adapter/create-system
                  (fn [args]
                    (reset! captured args)
                    ::built)]
      (is (= ::built (pilot/build-adapter :semantic spec)))
      (is (= {:index :index
              :store-config store-config
              :system-name "semantic"}
             @captured)))))

(deftest identity-capabilities-and-overlay-boundary
  (let [system (make-system)]
    (is (= "semantic" (ygp/system-id system)))
    (is (= :proximum (ygp/system-type system)))
    (is (= #{:snapshotable :branchable :graphable :committable}
           (ygp/capabilities system)))
    (is (satisfies? ygp/Snapshotable system))
    (is (satisfies? ygp/Branchable system))
    (is (satisfies? ygp/Graphable system))
    (is (satisfies? ygp/Committable system))
    (is (satisfies? ygp/GarbageCollectable system))
    (is (satisfies? ygp/Mergeable system))
    (is (= {:candidate-overlay/status :host-supplied
            :candidate-overlay/projection-role :semantic
            :candidate-overlay/authority :proposal-only
            :candidate-overlay/reason
            :proximum/no-reversible-multi-system-transaction}
           adapter/candidate-overlay-seam))))

(deftest constructor-requires-complete-host-binding
  (doseq [args [{:store-config store-config :system-name "semantic"}
                {:index :index :system-name "semantic"}
                {:index :index :store-config store-config}]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (adapter/create-system args)))))

(deftest snapshot-and-graph-arities-delegate
  (let [system (make-system)
        calls (atom [])]
    (with-redefs [prox/get-commit-id (fn [index] (:commit-id index))
                  prox/parents (fn [index] (if index #{:p0} #{}))
                  prox/get-branch (fn [index] (:branch index))
                  prox/load-commit
                  (fn [config commit-id opts]
                    (swap! calls conj [:load-commit config commit-id opts])
                    {:commit-id commit-id :branch (:branch opts)})
                  prox/commit-info
                  (fn [index commit-id]
                    {:index index :commit-id commit-id})
                  prox/branches (fn [_] #{:main :feature})
                  prox/history (fn [_] [:c0 :c1])
                  prox/ancestors (fn [_ commit-id] #{commit-id :c0})
                  prox/ancestor? (fn [_ a b] (= [a b] [:c0 :c1]))
                  prox/common-ancestor (fn [_ _a _b] :c0)
                  prox/commit-graph
                  (fn [_] {:branches {:main :c1} :commits #{:c0 :c1}})]
      (is (= :c0 (ygp/snapshot-id system)))
      (is (= #{:p0} (ygp/parent-ids system)))
      (is (= {:commit-id :c1 :branch :main}
             (ygp/as-of system :c1)))
      (is (= {:commit-id :c2 :branch :feature}
             (ygp/as-of system :c2
                        {:branch :feature :cache-size 32 :ignored true})))
      (is (= [[:load-commit store-config :c1 {:branch :main}]
              [:load-commit store-config :c2
               {:branch :feature :cache-size 32}]]
             @calls))
      (is (= :c1 (:commit-id (ygp/snapshot-meta system :c1))))
      (is (= :c2 (:commit-id (ygp/snapshot-meta system :c2 {}))))
      (is (= #{:main :feature} (ygp/branches system)))
      (is (= #{:main :feature} (ygp/branches system {})))
      (is (= [:c0 :c1] (ygp/history system)))
      (is (= [:c0 :c1] (ygp/history system {})))
      (is (= #{:c1 :c0} (ygp/ancestors system :c1)))
      (is (= #{:c2 :c0} (ygp/ancestors system :c2 {})))
      (is (true? (ygp/ancestor? system :c0 :c1)))
      (is (false? (ygp/ancestor? system :c1 :c0 {})))
      (is (= :c0 (ygp/common-ancestor system :c1 :c2)))
      (is (= :c0 (ygp/common-ancestor system :c1 :c2 {})))
      (is (= {:branches {:main :c1} :commits #{:c0 :c1}}
             (ygp/commit-graph system)))
      (is (= {:branches {:main :c1} :commits #{:c0 :c1}}
             (ygp/commit-graph system {})))
      (is (= :c1 (:commit-id (ygp/commit-info system :c1))))
      (is (= :c2 (:commit-id (ygp/commit-info system :c2 {})))))))

(deftest branch-arities-update-wrapped-index
  (let [system (make-system)
        calls (atom [])]
    (with-redefs [prox/get-branch (fn [index] (:branch index))
                  prox/load
                  (fn [config opts]
                    (swap! calls conj [:load config opts])
                    {:commit-id :loaded :branch (:branch opts)})
                  prox/load-commit
                  (fn [config commit-id opts]
                    (swap! calls conj [:load-commit config commit-id opts])
                    {:commit-id commit-id :branch (:branch opts)})
                  prox/branch!
                  (fn [index name]
                    (swap! calls conj [:branch (:commit-id index) name])
                    (assoc index :branch name))
                  prox/delete-branch!
                  (fn [index name]
                    (swap! calls conj [:delete name])
                    (assoc index :deleted name))]
      (let [from-current (ygp/branch! system :feature)
            from-branch (ygp/branch! system :copy :source)
            from-commit (ygp/branch! system :from-commit "c9"
                                     {:cache-size 8 :ignored true})
            deleted (ygp/delete-branch! system :old)
            deleted-with-opts (ygp/delete-branch! system :older {})
            checked-main (ygp/checkout system :main)
            checked-feature (ygp/checkout system :feature
                                          {:cache-size 4 :ignored true})]
        (is (= :main (ygp/current-branch system)))
        (is (= :main (ygp/current-branch from-current)))
        (is (= :main (ygp/current-branch from-branch)))
        (is (= :main (ygp/current-branch from-commit)))
        (is (= :main (ygp/current-branch deleted)))
        (is (= :main (ygp/current-branch deleted-with-opts)))
        (is (= :main (ygp/current-branch checked-main)))
        (is (= :feature (ygp/current-branch checked-feature)))
        (is (not (identical? system from-current)))
        (is (not (identical? system deleted)))
        (is (not (identical? system checked-feature))))
      (is (= [[:branch :c0 :feature]
              [:load store-config {:branch :source}]
              [:branch :loaded :copy]
              [:load-commit store-config "c9"
               {:cache-size 8 :branch :main}]
              [:branch "c9" :from-commit]
              [:delete :old]
              [:delete :older]
              [:load store-config {:branch :main}]
              [:load store-config {:branch :feature :cache-size 4}]]
             @calls)))))

(deftest commit-arities-block-update-and-return-system
  (let [system (make-system)
        calls (atom [])
        next-id (atom 0)]
    (with-redefs [prox/sync!
                  (fn [index opts]
                    (swap! calls conj [(:commit-id index) opts])
                    (channel-of (assoc index :commit-id
                                       (keyword (str "c" (swap! next-id inc))))))
                  prox/get-commit-id (fn [index] (:commit-id index))]
      (let [first-commit (ygp/commit! system)
            second-commit (ygp/commit! first-commit "second")
            third-commit (ygp/commit! second-commit "third"
                                      {:author "test"})]
        (is (= :c0 (ygp/snapshot-id system)))
        (is (= :c1 (ygp/snapshot-id first-commit)))
        (is (= :c2 (ygp/snapshot-id second-commit)))
        (is (= :c3 (ygp/snapshot-id third-commit)))
        (is (not (identical? system first-commit)))
        (is (not (identical? first-commit second-commit)))
        (is (= [[:c0 {}]
                [:c1 {:message "second"}]
                [:c2 {:author "test" :message "third"}]]
               @calls))))
    (with-redefs [prox/sync! (fn [_ _] (channel-of nil))]
      (let [data (try
                   (ygp/commit! system)
                   nil
                   (catch clojure.lang.ExceptionInfo error
                     (ex-data error)))]
        (is (= :proximum/sync-empty (:err data)))))))

(deftest unsupported-exact-gc-and-merge-fail-explicitly
  (let [system (make-system)]
    (with-redefs [prox/commit-graph
                  (fn [_]
                    {:branches {:main :c1 :feature :c2 :empty nil}})]
      (is (= #{:c1 :c2} (ygp/gc-roots system))))
    (doseq [[expected-op invoke]
            [['gc-sweep! #(ygp/gc-sweep! system #{:c0})]
             ['gc-sweep! #(ygp/gc-sweep! system #{:c0} {})]
             ['merge! #(ygp/merge! system :source)]
             ['merge! #(ygp/merge! system :source {})]
             ['conflicts #(ygp/conflicts system :a :b)]
             ['conflicts #(ygp/conflicts system :a :b {})]
             ['diff #(ygp/diff system :a :b)]
             ['diff #(ygp/diff system :a :b {})]]]
      (let [data (try
                   (invoke)
                   nil
                   (catch clojure.lang.ExceptionInfo error
                     (ex-data error)))]
        (is (= :proximum/unsupported-protocol-method (:err data)))
        (is (= :proximum (:backend data)))
        (is (= expected-op (:op data)))))))
