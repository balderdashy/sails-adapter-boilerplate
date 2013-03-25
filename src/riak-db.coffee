db = require('riak-js').getClient()
_ = require 'underscore'


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

    getAll: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#getAllModlesInCollection - Collection name must be provided.")
        else
            dbGetAll("#{@tag}_#{collectionName}")
                .then(
                    (result) ->
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


    save: promisify (collectionName, models, cb) ->
        if !collectionName? or !models?
            cb new Error("RiakDB#getAllModlesInCollection - Collection name must be provided.")
        else
            models = [models] if !(_.isArray models)
            deferred.map(models,
                    (model) =>
                        bucket = "#{@tag}_#{collectionName}"
                        dbSavePromise(bucket, model.id, model, { returnbody: true })
                            .then(
                                (result) ->
                                    result.shift()
                            )
                )
                .then(
                    (savedModels) ->
                        if savedModels.length isnt models.length
                            cb new Error("RiakDB#save - Saved only #{savedModels.length} models (out of #{models.length})")
                        else
                            cb null, savedModels
                ).end()




    get: promisify (collectionName, key, cb) ->
        if !collectionName? or !key?
            cb new Error("RiakDB#get - The collection name and the key must be provided.")
        else
            dbGetPromise("#{@tag}_#{collectionName}", key, {})
                .then(
                    (result) ->
                        cb null, result.shift
                    ,
                    (err) ->
                        if err.statusCode == 404
                            cb null, []
                        else
                            cb err
                )

    getCollections: promisify (cb) ->
        dbBucketsPromise()
            .then(
                (result) ->
                    cb null, result.shift()
                ,
                (err) ->
                    cb err
            )

    defineSchema: promisify (collectionName, definition, cb) ->
        unless (collectionName? && definition?)
            cb new Error('RiakDB#defineSchema - Collection name and schema definition must be provided.')
        else
            dbSavePromise("#{@tag}_schema", collectionName, definition, {})
                .then(
                    () ->
                        cb()
                    ,
                    (err) ->
                        cb err
                )


    describeSchema: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#defineSchema - Illegal argument.")
        else
            dbGetPromise("#{@tag}_schema", collectionName, {})
                .then(
                    (result) ->
                        cb null, result.shift()
                    ,
                    (err) ->
                        cb err
                )




