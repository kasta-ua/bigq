(defproject modnakasta/bigq "1.0.2"
  :description "Library for making requests to BigQuery"
  :url "https://github.com/modnakasta/bigq"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}

  :dependencies
  [[org.clojure/clojure "1.8.0"]
   [cheshire "5.7.0"]
   [clj-http "2.3.0"]
   [buddy/buddy-sign "1.4.0"]
   [com.taoensso/timbre "4.10.0"]])
