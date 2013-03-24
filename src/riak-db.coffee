db = require('riak-js').getClient()
_ = require 'underscore'


deferred = require 'deferred'
promisify = deferred.promisify

dbRemovePromise = _.bind promisify(db.remove), db
dbSavePromise = _.bind promisify(db.save), db
dbGetPromise = _.bind promisify(db.get), db
dbKeyCountPromise = _.bind promisify(db.count), db


module.exports = class RiakDB

    constructor: (@tag) ->

    getAllModelsInCollection: promisify (collectionName, cb) ->
        console.log "CHECK 0"
        unless collectionName?
            cb new Error("RiakDB#getAllModlesInCollection - Collection name must be provided.")
        else
            console.log "CHECK 0.0"
            db.mapreduce
                .add("#{@tag}_collectionName")
                .map('Riak.mapValuesJson')
                .run (err, models) ->
                    console.log "CHECK 0.1"
                    if err?
                        console.log "CHECK 1"
                        cb err
                    else
                        console.log "CHECK 2"
                        cb null, models


    getMaxIndex: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#getAllModlesInCollection - Collection name must be provided.")
        else
            db.mapreduce
                .add("#{@tag}_#{collectionName}")
                .map(
                    (v) ->
                        [(Riak.mapValuesJson(v)[0]).id ]
                )
                .reduce(
                    (v) ->
                        [Math.max.apply(null, v)]
                )
                .run (err, models) ->
                    if err?
                        cb err
                    else
                        cb null, models


    save: promisify (collectionName, models, cb) ->
        unless collectionName?
            cb new Error("RiakDB#getAllModlesInCollection - Collection name must be provided.")
        else
            models = [models] if !(_.isArray models)
            deferred.map(models)
                .then(
                    (model) ->
                        dbSavePromise("#{@tag}_#{collectionName}", model.id, model, { returnbody: true })
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




