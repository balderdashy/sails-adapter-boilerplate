deferred = require 'deferred'
promisify = deferred.promisify
_ = require 'underscore'

RiakDB = require '../lib/riak-db'

riakDB = new RiakDB('test')

riakDB.getAllModels('user')
    .end(
        (models) ->
            console.log "#{JSON.stringify models, null, '  '}"
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
