riakJS = require('riak-js')

_ = require 'underscore'

_.str = require 'underscore.string'
_.mixin _.str.exports()

deferred = require 'deferred'
promisify = deferred.promisify

TaskPool = require './task-pool'

module.exports = class RiakDB

    constructor: (collection) ->
        @tag = collection.dbTag

        db = riakJS.getClient {port: collection.port, host: collection.host}

        dbRemovePromise = _.bind promisify(db.remove), db
        dbSavePromise = _.bind promisify(db.save), db
        dbSaveBucketPromise = _.bind promisify(db.saveBucket), db
        dbGetPromise = _.bind promisify(db.get), db
        dbGetAll= _.bind promisify(db.getAll), db
        dbSearchPromise = _.bind promisify(db.search.find), db.search

        createModelTaskPools = {}

        @create = promisify (collectionName, model, cb) ->
            unless (collectionName? and model?)
                return cb new Error("RiakDB#create - Collection name and model definition must be provided.")

            createModelPromise = promisify (cb) ->
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
                                if (!model[attrName]?)
                                    # increment AI fields in values set
                                    model[attrName] = schema.autoIncrement
                                else
                                    if parseInt(model[attrName]) > schema.autoIncrement
                                        schema.autoIncrement = parseInt(model[attrName])
                                modelKey = model[attrName]
                                break

                        # update the collection schema with the new AI value
                        schema.autoIncrement += 1
                        @defineSchema(collectionName, schema, {})
                            .then(
                                (schema) ->
                                    deferred modelKey
                            )
                )
                .then(
                    (key) =>
                        @save(collectionName, key, model)
                )
                .end(
                    (model) ->
                        cb null, model
                    ,
                    (err) ->
                        cb err
                )

            if !createModelTaskPools[collectionName]?

                createModelTaskPools[collectionName] = {}
                createModelTaskPools[collectionName].taskPool = new TaskPool
                createModelTaskPools[collectionName].cbTable = {}

                createModelTaskPools[collectionName].taskPool.on 'task:complete',
                    (completedTaskId, model) =>
                        createModelTaskPools[collectionName].cbTable[completedTaskId].call(null, null, model)
                        delete createModelTaskPools[collectionName].cbTable[completedTaskId]

                createModelTaskPools[collectionName].taskPool.on 'error',
                    (failedTaskId, err) =>
                        createModelTaskPools[collectionName].cbTable[failedTaskId].call(null, err)
                        delete createModelTaskPools[collectionName].cbTable[failedTaskId]

                createModelTaskPools[collectionName].taskPool.on 'drain:complete',
                    =>
                        createModelTaskPools[collectionName].taskPool.removeAllListeners 'task:complete'
                        createModelTaskPools[collectionName].taskPool.removeAllListeners 'drain:complete'
                        createModelTaskPools[collectionName].taskPool.removeAllListeners 'error'
                        createModelTaskPools[collectionName] = null

            createModelTaskPools[collectionName].cbTable[createModelTaskPools[collectionName].taskPool.addTask(createModelPromise, [], @)] = cb
            createModelTaskPools[collectionName].taskPool.drain()


        @save = promisify (collectionName, key, model, cb) ->
            unless (collectionName? and key? and model?)
                return cb new Error("RiakDB#save - Collection name, key and model definition must be provided.")

            dbSavePromise("#{@tag}_#{collectionName}", key, model, { returnbody: true })
                .end(
                    (result) ->
                        cb null, result.shift()
                    ,
                    (err) ->
                        cb err
                )


        @get = promisify (collectionName, key, cb) ->
            unless (collectionName? and key?)
                return cb new Error("RiakDB#get - The collection name and the key must be provided.")

            dbGetPromise("#{@tag}_#{collectionName}", key, {})
                .end(
                    (result) =>
                        cb null, result.shift()
                    ,
                    (err) ->
                        if err.statusCode == 404
                            cb null
                        else
                            cb err
                )


        @remove = promisify (collectionName, key, cb) ->
            unless (collectionName? and key?)
                return cb new Error("RiakDB#delete - The collection name and the key must be provided.")

            dbRemovePromise("#{@tag}_#{collectionName}", key)
                .end(
                    (result) ->
                        cb null, result[1].key
                    ,
                    (err) =>
                        cb err
                )


        @deleteAll = promisify (collectionName, cb) ->
            unless collectionName?
                return cb new Error("RiakDB#deleteAll - The collection name must be provided.")

            @getAllKeys(collectionName)
                .then(
                    (keys) =>
                        deferred.map(keys,
                            (key) =>
                                @remove(collectionName, key)
                                    .then(
                                        (deletedKey) ->
                                            deletedKey
                                    )
                        )
                )
                .end(
                    (deletedKeys) ->
                        cb null, deletedKeys
                    ,
                    (err) ->
                        cb err
                )


        @getAllKeys = promisify (collectionName, cb) ->
            unless collectionName?
                return cb new Error("RiakDB#getAllKeys - Collection name must be provided.")

            keyStream = db.keys "#{@tag}_#{collectionName}", { keys: 'stream' }, undefined

            keyList = []
            keyStream.on 'keys', (keys) ->
                for key in keys
                    keyList.push key

            keyStream.on 'end', () ->
                cb null, keyList

            keyStream.start()


        @getAllModels = promisify (collectionName, cb) ->
            unless collectionName?
                return cb new Error("RiakDB#getAllModels - Collection name must be provided.")

            dbGetAll("#{@tag}_#{collectionName}")
                .end(
                    (result) =>
                        cb null, result.shift()
                    ,
                    (err) ->
                        cb err
                )


        @getMaxIndex = promisify (collectionName, cb) ->
            unless collectionName?
                return cb new Error("RiakDB#getAllModlesInCollection - Collection name must be provided.")

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


        @defineSchema = promisify (collectionName, definition, options, cb) ->
            unless (collectionName? and definition?)
                return cb new Error('RiakDB#defineSchema - Collection name and schema definition must be provided.')

            savedModel = null

            # save the schema
            dbSavePromise("#{@tag}_schema", collectionName, definition, { returnbody: true })
                .then(
                    (result) =>
                        savedModel = result.shift()
                        if options?.search is true
                            # create a search index for the collection
                            dbSaveBucketPromise("#{@tag}_#{collectionName}", {search: true})
                        else
                            deferred true
                )
                .end(
                    ->
                        cb null, savedModel
                    ,
                    (err) ->
                        cb err
                )


        @describeSchema = promisify (collectionName, cb) ->
            unless collectionName?
                return cb new Error("RiakDB#describeSchema - Collection name must be provided.")

            dbGetPromise("#{@tag}_schema", collectionName, {})
                .end(
                    (result) ->
                        cb null, result.shift()
                    ,
                    (err) ->
                        if err.statusCode == 404
                            cb null
                        else
                            cb err
                )


        @search = promisify (collectionName, searchTerm, cb) ->
            unless (collectionName? and searchTerm?)
                return cb new Error("RiakDB#searchInCollection - Collection name must and the search-term be provided.")

            dbSearchPromise("#{@tag}_#{collectionName}", "#{searchTerm}")
                .end(
                    (result) ->
                        cb null, result.shift()['docs']
                    ,
                    (err) ->
                        cb err
                )


        @deleteSchema = promisify (collectionName, cb) ->
            unless collectionName?
                return new Error("RiakDB#deleteSchema - Collection name must be provided.")

            dbRemovePromise("#{@tag}_schema", collectionName)
                .end(
                    (result) ->
                        cb null, result[1].key
                    ,
                    (err) ->
                        if err.statusCode == 404
                            cb null
                        else
                            cb err
                )






