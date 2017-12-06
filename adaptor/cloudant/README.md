# Cloudant adaptor

The Cloudant adaptor is capable of following the changes feed and it deals with
inserts, deletes, and updates.

## Configuration

```javascript
r = cloudant({
   "uri": "${CLOUDANT_URI}",
   //  "username": "username",
   //  "password": "password",
   //  "database": "database"
   //  "batchsize": int         // Sink only
   //  "timeout": int           // Sink only
   //  "seqInterval": int       // Source only
})
```

## Notes

A Cloudant database stores JSON documents. A document is stored as a revision tree
keyed on a pair of fields `_id` and `_rev` (both strings) that together uniquely
identifies a document revision. Only the leaves of the tree are guaranteed to be
present (in that way it's conceptually different from say Git).

Inserts, updates and deletes are all implemented as inserts. An update is generating
a new, complete revision under a new `_rev` but the same `_id` as the parent revision
being modified. A deletion is the same, but with the body discarded, and an additional
field `_deleted: true`. This new revision is called a `tombstone`.

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
a primary key, you may be able to use that as the `_id` for the Cloudant sink.

The Cloudant sink is efficiently implemented across multiple worker routines that
group writes into batches. The batch size is a configuration, as is the `timeout` which
represents the max time (seconds) we will allow a message to be waiting in the batch queue
before it's sent.

The Cloudant sink makes no attempts to handle update conflicts, should they occur.

## Source

The Cloudant source reads its changes feed as-is, with `include_docs=true` to get the
payloads, too. You can pass in a `seqInterval` integer as part of the configuration
which determines how often Cloudant should calculate the change sequence ID. In a
clustered scenario, setting this to say a 100 or larger represents a significant
performance boost, as it's expensive for Cloudant to calculate this. Returns diminish
though -- the biggest boost is going from 0 to 100. The downside is that the sequence
ID is also the resume granularity: it's the bookmark determining how far into the feed
you've gone.

You can run the Cloudant Adaptor in Batch or Tail mode. In Batch mode, a single request
fetches the whole of the changes feed in one go (it will stream the items). It's best
suitable for smallish, static datasets. In Tail mode, it will open the changes feed in
continuous mode, and keep running, capturing changes 'live' over time.

It's worth noting that the Cloudant changes feed cannot be relied upon to be ordered.
It multiplexes streams from different shards, so changes may appear out of order.

In Tail mode, the adaptor will keep running, and will resume from the last known good
point if the far end exits. It will, however, not persist this state. If you halt
restart the adaptor, it will start from the beginning. Transforms should be aware
of this.

