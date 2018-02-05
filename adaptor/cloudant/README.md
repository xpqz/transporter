# Cloudant adaptor

The Cloudant adaptor can function both as source and sink. The source can grab
the data in one go, and it can follow an evolving database continuously (tailing).

The sink has a naive version that writes each received document in turn, and it
has a bulk-loading version (which is much more efficient).

For real workloads use the tailing source and the bulking sink.

## Configuration

```javascript
r = cloudant({
    "uri": "${CLOUDANT_URI}",
    "username": "username",
    "password": "password",
    "database": "database",
    //  "batchsize": int,         // Sink only
    //  "batchtimeout": int,      // Sink only
    //  "seqinterval": int,       // Source only
    //  "tail": bool,             // Source only
    //  "newedits": bool,         // Sink only
    // Note: all cloudant URIs must be "cloudant://..."
});
```

## Notes

A Cloudant database stores JSON documents. A document is stored as a revision tree
keyed on a pair of fields `_id` and `_rev` (both strings) that together uniquely
identifies a document revision. Only the leaves of the tree are guaranteed to be
present in terms of payload (in that way it's conceptually different from say Git).

In Cloudant/CouchDB, inserts, updates and deletes are all implemented as inserts.
An update is generating a new, complete revision under a new `_rev` but the same
`_id` as the parent revision being modified. A deletion is the same, but with the
body discarded, and an additional field `_deleted: true`. This new revision is
called a `tombstone`.

If no `_id` is given when inserting a document into a Cloudant database, one will be
automatically generated. The `_rev` is generated, too, and is in simplified terms a
hash of the body. In order to update (or delete) a document you need to provide both the
`_id` and the `_rev` of the document to be updated (or deleted), and in order for this
operation to succeed, the revision identified by `{_id, _rev}` must be a leaf, or the
update (delete) will be rejected as a conflict.

## Sink

If you use the Cloudant adaptor, you need to ensure that each message destined for
a Cloudant sink needs to have both an `_id` and a `_rev` in order to update or delete.
Inserts need neither, but an `_id` may be provided. If your source has the concept of
a primary key, you may want to use that as the `_id` for the Cloudant sink.

The bulk version of the Cloudant sink is implemented as a queue that drains either at
regular time intervals (`batchtimeout`), or when it reaches a configurable max
occupancy (`batchsize`).

The Cloudant sink makes no attempts to handle update conflicts, should they occur.

If you're running a Cloudant source to a Cloudant sink you need to set `newedits` to
`false` on the sink in order to preserve `_rev` ids.

Note that Cloudant has a max 1MB restriction on request body size. The Cloudant
sink does not protect against this.

## Source

You can run the Cloudant Adaptor in Batch or Tail mode. In Batch mode, a single request
fetches the whole of the changes feed in one go (it will stream the items). It's best
suitable for smallish, static datasets. In Tail mode, it will open the changes feed in
continuous mode, and keep running, capturing changes 'live' over time.

In tail mode you can pass in a `seqinterval` integer as part of the configuration
which determines how often Cloudant should calculate the change sequence ID. In a
clustered scenario, setting this to say a 100 or larger represents a significant
performance boost, as it's expensive for Cloudant to calculate this. Returns diminish
though -- the biggest boost is going from 0 to 100. The downside is that the sequence
ID is also the resume granularity: it's the bookmark determining how far into the feed
you've gone.

It's worth noting that the Cloudant changes feed cannot be relied upon to be ordered.
It multiplexes streams from different shards, so changes may appear out of order.

In Tail mode, the adaptor will keep running, and will resume from the last known good
point if the far end exits. It will, however, not persist this state, at the time of
writing. If you halt restart the adaptor, it will start from the beginning. Transforms
should be aware of this.

When using a Cloudant source, you will also pick up any design documents (documents
where the `_id` starts with `_design/`) which describes secondary indexes. If you are
moving documents to a non-Cloudant sink, you may want to filter these out.
