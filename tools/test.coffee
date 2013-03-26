deferred = require 'deferred'
promisify = deferred.promisify
_ = require 'underscore'

RiakDB = require '../lib/riak-db'

dbTag = 'transactions'
collection = 'schema'

riakDB = new RiakDB(dbTag)

riakDB.getAllModels(collection)
    .then(
        (models) ->
            console.log "Models: #{JSON.stringify models, null, '  '}"
            riakDB.getAllKeys(collection)
    )
    .then(
        (keys) ->
            console.log "Keys: #{JSON.stringify keys, null, '  '}"
            riakDB.resetDB()
    )
    .end(
        (bucketKeyPairs) ->
            console.log "Items deleted: #{bucketKeyPairs.length}"
    )

#riakDB.getCollections()
#    .then(
#        (collections) ->
#            console.log "Available collections: #{JSON.stringify collections}"
#
#            deferred.map(collections,
#                (collection) ->
#                    riakDB.deleteAll(collection)
#                        .then(
#                            (deletedKeys) ->
#                                {collection: collection, keys: deletedKeys}
#                        )
#            )
#    )
#    .end(
#        (data) ->
#            console.log "Keys deleted: #{JSON.stringify data}"
#        ,
#        (err) ->
#            console.log "ERROR: #{JSON.stringify err, null, '  '}"
#    )
