(ns sv.datomic-lease.core
  "Provides
   a [lease](https://en.wikipedia.org/wiki/Lease_(computer_science))
   that is based on Datomic. The lease supports reentrants, meaning
   the current lease holder can acquire the lease again.

   Due to the distributed nature of this lease implementation it
   cannot ensure 100% mutual exclusion (long GC pauses, high latency
   etc.).

   There are techniques
   like [fencing](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
   that can help to ensure mutal exclusion, if the system that does the
   final side effect uses the fencing token in combination with an
   atomic compare-and-swap operation.

   Here a similar approach:
   > There must be some means of notifying the lease holder of the
   > expiration and preventing that agent from continuing to rely on the
   > resource. Often, this is done by requiring all requests to be
   > accompanied by an access token, which is invalidated if the
   > associated lease has expired.
   https://en.wikipedia.org/wiki/Lease_(computer_science)#Problems

   Here an approach for systems that do not provide an atomic
   compare-and-swap: acquire the lease, prepare everything, right
   before doing the critical side effect (like configure-to-story),
   acquire the lease again (reentrant). This minimizes the chances that
   a long GC pause or some slow network resource causes an expired
   lease before the side effect is executed.
   "
  (:require [datomic.api :as d]))

(def ^:private transaction-functions
  [{:db/ident :fn/retract-if-match
    :db/fn (d/function
             {:lang "clojure"
              :params '[db e a v]
              :code '(let [[_ _ current-v] (first (d/datoms db :eavt e a))]
                       (when (= current-v
                                v)
                         [[:db/retract e a v]]))})}

   {:db/ident :fn/lease
    :db/fn (d/function
             {:lang "clojure"
              :params '[db e a old-v new-v ttl-ms]
              :code '(let [[_ _ v tx] (first (d/datoms db :eavt e a))]
                       (when (= v new-v)
                         (throw (ex-info "new-v should not equal to current v"
                                         {:error :invalid-new-v})))
                       (if (or
                             ;; no one has the lease, acquire it:
                             (nil? v)
                             ;; transaction creator still has the
                             ;; lease (reentrant), extend it:
                             (= old-v v)
                             ;; check if lease has expired:
                             (< (+ (.getTime (:db/txInstant (d/entity db tx)))
                                   ttl-ms)
                                (System/currentTimeMillis)))
                         [[:db/add e a new-v]]
                         (throw (ex-info "not expired"
                                         {:error :not-expired}))
                         ))})}])

(defn new-lease
  [{:keys [e a con ttl-ms]}]
  (when-not (d/entid (d/db con)
                     :fn/lease)
    @(d/transact con
                 transaction-functions))
  (let [state (atom {:uuid nil})]
    {:lease (fn []
              (let [new-v (d/squuid)
                    old-v (:uuid @state)
                    start (System/currentTimeMillis)]
                (try
                  @(d/transact con
                               [[:fn/lease e a old-v new-v ttl-ms]])
                  (swap! state
                         assoc
                         :uuid
                         new-v)
                  (let [remaining (- (+ start ttl-ms)
                                     (System/currentTimeMillis))]
                    (if (< 0 remaining)
                      {:uuid new-v
                       :remaining-ms remaining}
                      false))
                  (catch Exception e
                    (if (= (:error (ex-data (.getCause e)))
                           :not-expired)
                      false
                      (throw e))))))
     :release (fn []
                (when-let [uuid (:uuid @state)]
                  @(d/transact con
                               [[:fn/retract-if-match e a uuid]]))
                (reset! state
                        {:uuid nil})
                true)
     :state state}))
