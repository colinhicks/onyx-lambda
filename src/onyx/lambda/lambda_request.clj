(ns onyx.lambda.lambda-request
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:import [com.amazonaws.services.lambda.runtime Context]))

(defn body [in]
  (-> in
      io/reader
      json/read
      clojure.walk/keywordize-keys))

(defn context-map [^Context context]
  {:memory-limit-in-mb (.getMemoryLimitInMB context)
   :client-context (.getClientContext context)
   :log-stream-name (.getLogStreamName context)
   :log-group-name (.getLogGroupName context)
   :identity (.getIdentity context)
   :function-name (.getFunctionName context)
   :function-version (.getFunctionVersion context)
   :invoked-function-arn (.getInvokedFunctionArn context)
   :aws-request-id (.getAwsRequestId context)
   :remaining-time-in-millis (.getRemainingTimeInMillis context)
   :logger (.getLogger context)})

(defn write-output [lambda-request]
  (with-open [w io/writer]
    (json/write (:output-stream lambda-request) w)))

(defrecord LambdaRequest [body output-stream context])

;; TODO connect timbre to log4j / lambda appender
;; http://docs.aws.amazon.com/lambda/latest/dg/java-logging.html#java-wt-logging-using-log4j

(defn lambda-request [input-stream output-stream context]
  (->LambdaRequest (body input-stream)
                   (context-map context)
                   output-stream)) 
