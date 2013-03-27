db = require('riak-js').getClient()
_ = require 'underscore'

_.str = require 'underscore.string'
_.mixin _.str.exports()

deferred = require 'deferred'
promisify = deferred.promisify

dbRemovePromise = _.bind promisify(db.remove), db
dbSavePromise = _.bind promisify(db.save), db
dbGetPromise = _.bind promisify(db.get), db
dbKeyCountPromise = _.bind promisify(db.count), db
dbGetAll= _.bind promisify(db.getAll), db
dbBucketsPromise = _.bind promisify(db.buckets), db


module.exports = class RiakDB

    constructor: (@tag) ->

    createModelQueue: {}

    create: promisify (collectionName, model, cb) ->
        if !collectionName? or !model?
            return cb new Error("RiakDB#create - Collection name and model definition must be provided.")

        drain = (collectionName) ->
            re

        @createModelLock[collectionName] = {working: false, queue: [] } unless @createModelLock[collectionName]?
        @createModelLock[collectionName].queue.push model

        else
            @createModelLock[collectionName] = true
            # Lookup collection schema so we know all of the attribute
            # names and the current auto-increment value
            @describeSchema(collectionName)
                .then(
                    (schema) =>
                        unless schema?
                            throw new Error("Cannot get schema for collection: #{collectionName} for DB instance: #{@tag}")

                        # Determine the attribute names which will be included in the created object
                        attrNames = _.keys _.extend({}, schema.attributes, model)

                        for attrName in attrNames
                            # But only if the given auto-increment value
                            # was NOT actually specified in the value set,
                            if (_.isObject(schema.attributes[attrName]) && schema.attributes[attrName].autoIncrement)
                                if (!model[attrName])
                                    # increment AI fields in values set
                                    model[attrName] = schema.autoIncrement
                                else
                                    if parseInt(model[attrName]) > schema.autoIncrement
                                        schema.autoIncrement = parseInt(model[attrName])
                                break

                        # update the collection schema with the new AI value
                        schema.autoIncrement += 1
                        @defineSchema(collectionName, schema)
                )
                .then(
                    (schema) =>
                        key = schema.autoIncrement - 1
                        @save(collectionName, key, model)
                )
                .end(
                    (model) =>
                        @createModelLock[collectionName] = false
                        cb null, model
                    ,
                    (err) =>
                        @createModelLock[collectionName] = false
                        cb err
                )


    save: promisify (collectionName, key, model, cb) ->
        if !collectionName? or !key? or !model?
            cb new Error("RiakDB#save - Collection name, key and model definition must be provided.")
        else
            dbSavePromise("#{@tag}_#{collectionName}", key, model, { returnbody: true })
                .then(
                    (result) =>
                        cb null, result.shift()
                    ,
                    (err) ->
                        cb err
                )


    get: promisify (collectionName, key, cb) ->
        if !collectionName? or !key?
            cb new Error("RiakDB#get - The collection name and the key must be provided.")
        else
            dbGetPromise("#{@tag}_#{collectionName}", key, {})
                .then(
                    (result) =>
                        cb null, result.shift()
                    ,
                    (err) ->
                        if err.statusCode == 404
                            cb null, null
                        else
                            cb err
                )


    delete: promisify (collectionName, key, cb) ->
        if !collectionName? or !key?
            cb new Error("RiakDB#delete - The collection name and the key must be provided.")
        else
            dbRemovePromise("#{@tag}_#{collectionName}", key)
                .then(
                    (result) ->
                        cb null, result[1].key
                    ,
                    (err) ->
                        cb err
                )


    deleteAll: promisify (collectionName, cb) ->
        if !collectionName?
            cb new Error("RiakDB#deleteAll - The collection name must be provided.")
        else
            @getAllKeys(collectionName)
                .then(
                    (keys) =>
                        deferred.map(keys,
                            (key) =>
                                @delete(collectionName, key)
                                    .then(
                                        (deletedKey) ->
                                            deletedKey
                                    )
                        )
                )
                .then(
                    (deletedKeys) ->
                        cb null, deletedKeys
                    ,
                    (err) ->
                        cb err
                )
                .end()


    getCollections: promisify (cb) ->
        dbBucketsPromise()
            .end(
                (result) =>
                    collections = []

                    _.map result.shift(), (bucket) =>
                        collections.push bucket.split('_')[1] if _(bucket).startsWith @tag

                    cb null, collections
                ,
                (err) ->
                    cb err
            )


    getAllKeys: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#getAllKeys - Collection name must be provided.")
        else
            keyStream = db.keys "#{@tag}_#{collectionName}", { keys: 'stream' }, undefined

            keyList = []
            keyStream.on 'keys', (keys) ->
                for key in keys
                    keyList.push key

            keyStream.on 'end', () ->
                cb null, keyList

            keyStream.start()


    getAllModels: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#getAllModels - Collection name must be provided.")
        else
            dbGetAll("#{@tag}_#{collectionName}")
                .then(
                    (result) =>
                        cb null, result.shift()
                    ,
                    (err) ->
                        cb err
                )


    getMaxIndex: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#getAllModlesInCollection - Collection name must be provided.")
        else
            db.mapreduce
                .add("#{@tag}_#{collectionName}")
                .map(
                    (riakObj) ->
                        [(Riak.mapValuesJson(riakObj)[0]).id ]
                )
                .reduce(
                    (v) ->
                        [Math.max.apply(null, v)]
                )
                .run (err, maxIndex) ->
                    if err?
                        cb err
                    else
                        cb null, maxIndex[0];


    defineSchema: promisify (collectionName, definition, cb) ->
        unless (collectionName? && definition?)
            cb new Error('RiakDB#defineSchema - Collection name and schema definition must be provided.')
        else
            dbSavePromise("#{@tag}_schema", collectionName, definition, { returnbody: true })
                .then(
                    (result) =>
                        cb null, result.shift()
                    ,
                    (err) ->
                        cb err
                )


    describeSchema: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#defineSchema - Collection name must be provided.")
        else
            dbGetPromise("#{@tag}_schema", collectionName, {})
                .then(
                    (result) ->
                        cb null, result.shift()
                    ,
                    (err) ->
                        if err.statusCode == 404
                            cb null
                        else
                            cb err
                )


    deleteSchema: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#deleteSchema - Collection name must be provided.")
        else
            dbRemovePromise("#{@tag}_schema", collectionName)
                .then(
                    (result) ->
                        cb null, result[1].key
                    ,
                    (err) ->
                        if err.statusCode == 404
                            cb null
                        else
                            cb err
                )

    resetDB: promisify (cb) ->
        dbBucketsPromise()
            .end(
                (result) ->
                    buckets = result.shift()
                    console.log "Bucket list: #{JSON.stringify buckets}"
                    deferred.map(buckets,
                        (bucket) ->
                            (promisify((bucket, cb) ->
                                keyStream = db.keys bucket, { keys: 'stream' }, undefined

                                keyList = []
                                keyStream.on 'keys', (keys) ->
                                    for key in keys
                                        keyList.push key

                                keyStream.on 'end', () ->
                                    cb null, keyList

                                keyStream.start()
                            )(bucket))
                            .then(
                                (keys) =>
                                    bucketKeyPairs = []
                                    for key in keys
                                        bucketKeyPairs.push {bucket: bucket, key: key}

                                    deferred bucketKeyPairs
                            )
                    )
                    .then(
                        (bucketKeyPairSets) ->
                            bucketKeyPairs = _.flatten bucketKeyPairSets
                            deferred.map(bucketKeyPairs,
                                (bucketKeyPair) ->
                                    dbRemovePromise(bucketKeyPair.bucket, bucketKeyPair.key)
                                        .then(
                                            (result) ->
                                                deferred {bucket: bucketKeyPair.bucket, key: result[1].key}
                                        )
                            )
                    )
                    .then(
                        (bucketKeyPairs) ->
                            cb null, bucketKeyPairs
                    )
                ,
                (err) ->
                    cb err
            )







