(ns com.stuartsierra.parallel
  (:require [clojure.core.async :refer [go go-loop <! >! alts! close!]]))

(defn parallel
  "Processes values from input channel in parallel on n 'go' blocks.

  Invokes f on values taken from input channel. Values returned from f
  are written on output channel.

  Returns a channel which will be closed when the input channel is
  closed and all operations have completed.

  Note: the order of outputs may not match the order of inputs."
  [n f input output]
  (let [tasks (doall
               (repeatedly n
                #(go-loop []
                   (let [in (<! input)]
                     (when-not (nil? in)
                       (let [out (f in)]
                         (when-not (nil? out)
                           (>! output out))
                         (recur)))))))]
    (go (doseq [task tasks]
          (<! task)))))

(defn pmax
  "Process messages from input in parallel with at most max concurrent
  operations.

  Invokes f on values taken from input channel. f must return a
  channel, whose first value (if not closed) will be put on the output
  channel.

  Returns a channel which will be closed when the input channel is
  closed and all operations have completed.

  Creates new operations lazily: if processing can keep up with input,
  the number of parallel operations may be less than max.

  Note: the order of outputs may not match the order of inputs."
  [max f input output]
  (go-loop [tasks #{input}]
    (when (seq tasks)
      (let [[value task] (alts! (vec tasks))]
        (if (= task input)
          (if (nil? value)
            (recur (disj tasks task))  ; input is closed
            (recur (conj (if (= max (count tasks))  ; max - 1 tasks running
                           (disj tasks input)  ; temporarily stop reading input
                           tasks)
                         (f value))))
          ;; one processing task finished: continue reading input
          (do (when-not (nil? value) (>! output value))
              (recur (-> tasks (disj task) (conj input)))))))))
