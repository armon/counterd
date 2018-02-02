# counterd daemon

This project implements a simple daemon for counting unique events for a set of attributes. The counterd accepts events that looks like:

```json
{
    "id": "3D8125BD-BEE4-4E90-A15F-81F42C380C55",
    "date": "2018-01-31T01:12:53Z",
    "attributes": {
        "foo": "bar",
        "zip": "zap"
    }
}
```

Where the `id` unique identifies the event, and the `attributes` can be an arbitrary set of key/value pairs.  The `date` can be omitted and the server will substitute in the current time.

When a new event is received, it is turned into a counter key and then corresponding HyperLogLog key in Redis is updated.  For the above event the set of keys that would be updated are:

* `day:2018-01-31:foo:bar:zip:zap`
* `week:2018-01-28:foo:bar:zip:zap`
* `month:2018-01:foo:bar:zip:zap`

By using a HyperLogLog key in Redis, counterd can handle many thousands of updates per second. The tradeoff is that the count of unique events is not perfectly accurate. However, this value is accurate within a few percentage points. See the [Redis documentation](https://redis.io/commands/pfcount) for more details.

To make the counters more usable, counterd supports snapshotting the counter values into a PostgreSQL database. When a snapshot is taken the domain of key/value pairs seen is updated in the `attributes_domain` table, and the counters are updated in the `counters` table.

Here is an example of each:

```sql
select * from attributes_domain;
 attribute |   value
-----------+-----------
 foo       | bar
 zip       | zap
(2 rows)

select * from counters limit 5;
                  id                  | interval |        date         |           attributes            | count
--------------------------------------+----------+---------------------+---------------------------------+-------
 0b9e8f7e-6f61-43c9-9f93-cc4a2a28cd80 | day      | 2018-01-30 00:00:00 | {"foo": "bar", "zip": "zap"}    |   406
```

This format makes it easy to query for the sum across various attributes:

```sql
select sum(count) from counters where attributes->'foo' = '"bar"';
  sum
-------
  406
(1 row)
```

# Usage

The `counterd` command has a few subcommands:

    * server: Runs a long lived daemon which serves the API and can optionally snapshot periodically
    * snapshot: Used to snapshot the counters and update the database
    * sim: Used to simulate input to the server API. Used for testing and benchmarking.
    * dbinit: Used to initialize the database and create the needed tables.

Each command documents the arguments. All the commands share an input file which is defined in
HCL or [HashiCorp Configuration Language](https://github.com/hashicorp/hcl). Below is an example file:

```hcl

// Configures the listen address for the API server. Below is the default.
listen_address = "127.0.0.1:8001"

// Configures the address of the redis server to use. Below is the default.
redis_address = "127.0.0.1:6379

// Provides the address of the postgresql database in URL format. Below is the default.
postgresql_address = "postgres://postgres@localhost/postgres?sslmode=disable",

// Configure details of the snapshot
snapshot {
    // Configures how often the server daemon should perform snapshotting.
    // By default this is blank, and snapshotting is disabled. The cron syntax
    // is documented here: https://godoc.org/github.com/robfig/cron
    cron = "@hourly"

    // Configures which counter values to update in the database. The update threshold
    // is how long before the current time to scan for counters and update the database.
    // As an example, if set to "24h", all counters that could have been modified by
    // a date in the last 24h will be updated. Defaults to 3 hours.
    update_threshold = "3h"

    // Configures which counter values to delete from redis. The delete threshold
    // is how long before the current time to scan for counters and delete from redis.
    // As an example, if set to "2232h" (e.g. 3 months), all counters older than then
    // would be deleted. Defaults to 3 months.
    delete_threshold = "2232h"
}

// Configure optional authentication
auth {
    // Required is used to optionally enable authentication. When enabled, an API client
    // must provide an "Authorization: Bearer <Token>" header to authenticate. Defaults to false.
    required = false

    // Tokens is a list of bearer tokens that are authorized to use the API.
    // Any number of tokens can be specified.
    tokens = ["D0816608-AB58-4AC8-9563-8D9F13B2F89D", "31937DCC-748A-4F4C-B568-016E3293B60D"]
}

// Configure optional filtering of attributes
attributes {
    // Whitelist is used to filter the set of attribute keys to only those explicitly in the list.
    // Any other attribute keys will be ignored.
    whitelist = ["foo", "bar"]

    // Blacklist is used to filter the set of attribute keys to exclude those in the list.
    // Any other attribute keys will be allowed.
    blacklist = ["zip"]
}
```

