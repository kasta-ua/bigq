(defproject modnakasta/bigq "1.1.2"
  :description "Library for making requests to BigQuery"
  :url "https://github.com/kasta-ua/bigq"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}

  :dependencies
  [[org.clojure/clojure "1.10.1" :scope "provided"]
   [metosin/jsonista "0.2.3"]
   [clj-http "3.10.0"]
   [buddy/buddy-sign "3.1.0"]
   [com.taoensso/timbre "4.10.0"]])
