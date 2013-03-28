deferred = require 'deferred'
promisify = deferred.promisify
_ = require 'underscore'

RiakDB = require '../lib/riak-db'

dbTag = 'test'
collection = 'user'

riakDB = new RiakDB(dbTag)

for i in [1...500]
    do (i) ->
        riakDB.describeSchema(collection)
            .then(
                (schema) ->
                    console.log "Described schema for collection: #{collection}"
                    riakDB.create(collection, {id: i})
            )
            .then(
                (model) ->
                    console.log "Model created: #{JSON.stringify model}"
                    riakDB.getAllModels collection
            )
            .then(
                (models) ->
                    console.log "Found #{models.length} models."
                    riakDB.getAllModels collection
            )
            .then(
                (models) ->
                    console.log "Found #{models.length} models."
                    riakDB.delete collection, i
            )
            .end(
                (key) ->
                    console.log "Deleted model with key: #{key}"
                    console.log "---------------------"
                ,
                (err) ->
                    console.log "ERROR: #{err.message}"
                    console.log "---------------------"
            )

#riakDB.getAllModels(collection)
#    .then(
#        (models) ->
#            console.log "Models: #{JSON.stringify models, null, '  '}"
#            riakDB.getAllKeys(collection)
#    )
#    .then(
#        (keys) ->
#            console.log "Keys: #{JSON.stringify keys, null, '  '}"
#            riakDB.resetDB()
#    )
#    .end(
#        (bucketKeyPairs) ->
#            console.log "Items deleted: #{bucketKeyPairs.length}"
#    )

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
