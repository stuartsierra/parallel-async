(ns com.stuartsierra.parallel-test
  (:require [com.stuartsierra.parallel :refer :all]
            [clojure.core.async :refer [go go-loop <! >! <!! close!
                                        chan to-chan timeout thread]]
            [clojure.test :refer [deftest is]]))

(defn sink
  "Returns an atom containing a vector. Consumes values from channel
  ch and conj's them into the atom."
  [ch]
  (let [a (atom [])]
    (go-loop []
      (let [val (<! ch)]
        (when-not (nil? val)
          (swap! a conj val)
          (recur))))
    a))

(defn watch-counter [counter thread-counts]
  (add-watch counter
             :thread-count
             (fn [_ _ _ thread-count]
               (swap! thread-counts conj thread-count))))

(deftest t-parallel
  (let [input (to-chan (range 50))
        output (chan)
        result (sink output)
        max-threads 5
        counter (atom 0)
        f (fn [x]
            (swap! counter inc)
            (Thread/sleep (rand-int 100))
            (swap! counter dec)
            x)
        thread-counts (atom [])]
    (watch-counter counter thread-counts)
    (<!! (parallel max-threads f input output))
    ;; First, we want to make sure that the input matches the output,
    ;; but we can't assume they are in the same order, so we convert
    ;; to sets.
    (is (= (set (range 50)) (set @result)))
    ;; Then, we want to make sure that the thread count never got
    ;; above our stipulated maximum.
    (is (every? #(<= % max-threads) @thread-counts))))

(deftest t-pmax-go
  (let [input (to-chan (range 50))
        output (chan)
        result (sink output)
        max-threads 5
        counter (atom 0)
        f (fn [x]
            (go
             (swap! counter inc)
             (<! (timeout (rand-int 100)))
             (swap! counter dec)
             x))
        thread-counts (atom [])]
    (watch-counter counter thread-counts)
    (<!! (pmax max-threads f input output))
    (is (= (set (range 50)) (set @result)))
    (is (every? #(<= % max-threads) @thread-counts))))

(deftest t-pmax-thread
  (let [input (to-chan (range 50))
        output (chan)
        result (sink output)
        max-threads 5
        counter (atom 0)
        f (fn [x]
            (thread
             (swap! counter inc)
             (<!! (timeout (rand-int 100)))
             (swap! counter dec)
             x))
        thread-counts (atom [])]
    (watch-counter counter thread-counts)
    (<!! (pmax max-threads f input output))
    (is (= (set (range 50)) (set @result)))
    (is (every? #(<= % max-threads) @thread-counts))))

(deftest t-pmax-slow-input
  (let [input (chan)
        output (chan)
        result (sink output)
        max-threads 5
        actual-needed-threads 3
        counter (atom 0)
        f (fn [x]
            (go
             (swap! counter inc)
             (<! (timeout (rand-int 100)))
             (swap! counter dec)
             x))
        thread-counts (atom [])]
    (watch-counter counter thread-counts)
    ;; Slow input:
    (go-loop [i 0]
      (if (< i 50)
        (do (<! (timeout 50))
            (>! input i)
            (recur (inc i)))
        (close! input)))
    (<!! (pmax max-threads f input output))
    (is (= (set (range 50)) (set @result)))
    (is (every? #(<= % actual-needed-threads) @thread-counts))))
