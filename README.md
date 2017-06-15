# bigq

`bigq` is a simplest possible library for making requests to BigQuery. There is
no Java client code inside, just calls to BigQuery's HTTP API. You give it
credentials and a query, it authorizes itself and then executes a query and
waits for it to end:

```clj
(bigq/query "path/to/auth.json" {:query "select count(*) from my.table"})
```

That's all! It's not possible to pass access token from outside, it's not
handling access token expiration (it just generates new one every time) - we're
using this library for scripts running from cron.
