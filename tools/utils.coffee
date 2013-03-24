async = require 'async'
_ = require 'underscore'

db = require('riak-js').getClient()
deferred = require 'deferred'
promisify = deferred.promisify

dbRemovePromise = _.bind promisify(db.remove), db
dbSavePromise = _.bind promisify(db.save), db
dbKeyCountPromise = _.bind promisify(db.count), db

asyncSeriesPromise = promisify async.series

class Utils
    @deleteAllKeys: promisify (bucket, cb) ->
        console.log "Deleting all entries in bucket: #{bucket}"
        keyStream = db.keys bucket, { keys: 'stream' }, undefined

        keyList = []
        keyStream.on 'keys', (keys) ->
            for key in keys
                keyList.push key

        keyStream.on 'end', () ->
            deferred.map(keyList,
                (key) ->
                    dbRemovePromise(bucket, key).then(
                        (meta, modelInfo) ->
                            modelInfo
                    )
            )
            .then(
                 (models) ->
                     cb null, models
                 ,
                 (err) ->
                     cb err
            )
            .end()


        keyStream.start()


    @showAllKeys: promisify (bucket, cb) ->
        keyStream = db.keys bucket, { keys: 'stream' }, undefined

        keyList = []
        keyStream.on 'keys', (keys) ->
            for key in keys
                keyList.push key

        keyStream.on 'end', () ->
            cb null, keyList

        keyStream.start()


    @countKeys: promisify (bucket, cb) ->
        dbKeyCountPromise(bucket)
        .then(
            (results) ->
                cb null, results.shift() if cb?
            ,
            (err) ->
                console.log "ERROR: #{JSON.stringigy err, null, '  '}"
                cb err if cb?
        )

    @createKeys: promisify (bucket, count, cb) ->
        deferred.map([1..count], 
            (id) ->
                model =
                    id: id

                dbSavePromise(bucket, id, model, {returnbody: true}).then(
                    (result) ->
                        result.shift()
                )
        )
        .then(
            (models) ->
                cb null, models
            ,
            (err) ->
                cb err
        )
        .end()


buckets = ['test_user', 'test_schema', 'test_index', 'user']
settle_time = 5 * 1000

deferred.monitor 30 * 1000, (err) ->
    console.log JSON.stringify err, null, ' '

#Utils.createKeys(bucket, 1000)
#    .then(
#        (models) ->
#            console.log "Created #{models.length} DB entries."
#            promisify(
#                (cb) ->
#                    setTimeout(()->
#                        cb null, bucket
#                    , settle_time)
#            )()
#    )
#    .then(
#        (bucket) ->
#            Utils.countKeys bucket
#    )
#    .then(
#        (keyCount) ->
#            console.log "After create, there are #{keyCount} keys in the '#{bucket}' bucket"
#            Utils.deleteAllKeys bucket
#    )
#    .then(
#        (models) ->
#            console.log "Deleted #{models.length} DB entries."
#            promisify(
#                (cb) ->
#                    setTimeout(()->
#                        cb null, bucket
#                    , settle_time)
#            )()
#    )
#    .then(
#        (bucket) ->
#            Utils.countKeys bucket
#    )
#    .then(
#        (keyCount) ->
#            console.log "After delete, there are #{keyCount} keys in the '#{bucket}' bucket"
#    )
#    .end()

deferred.map(buckets,
    (bucket) ->
        Utils.deleteAllKeys(bucket)
            .then(
                (models) ->
                    console.log "Deleted #{models.length} DB entries in the #{bucket} bucket."
            )
    )
    .then(
        () ->
            console.log 'Done.'
    ,
    (err) ->
        cb err
    )
    .end()