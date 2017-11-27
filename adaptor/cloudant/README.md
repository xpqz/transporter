# Cloudant adaptor

The Cloudant adaptor is capable of following the changes feed and it deals with
inserts, deletes, and updates.

### Configuration:
```javascript
r = cloudant({
    "uri": "cloudant://127.0.0.1:5984/",
//  "uri": "cloudant://{username}:{password}@{username}.cloudant.com/{database}",
//  "user": "username",
//  "password": "password",
//  "database": "database"
})
```
