############################################################
# :: sails-riak
# -> adapter
############################################################

MAX_INTEGER = 4294967295
TRANSACTION_WARNING_TIMEOUT = 2000;

deferred = require 'deferred'
promisify = deferred.promisify

_ = require 'underscore'
_.str = require 'underscore.string'
_.mixin _.str.exports()

uuid = require 'node-uuid'

async = require 'async'
asyncSeriesPromise = promisify async.series

RiakDB = require './riak-db'

module.exports = do () ->
    # Load criteria module
    getMatchIndices = require './criteria'

    # Maintain a list of active DB objects
    connections = { }

    adapter =
        # Whether this adapter is syncable (yes)
        syncable: true


        # Enable transactions by allowing Riak to create a
        # bucket for commitLog entries
        commitLog:
            identity: 'commit_log'
            adapter: 'sails-riak'
            dbTag: 'transactions'
            port: 8098
            host: 'localhost'
            migrate: 'drop'
            funcTable: {}


        # Default configuration for collections
        # (same effect as if these properties were included at the top level of the model definitions)
        defaults:
            port: 8098
            host: 'localhost'
            migrate: 'drop'

            # DB namespace: a prefix pre-pended to all Riak buckets
            dbTag: 'test'


        # This method runs when a model is initially registered at server start time
        registerCollection: (collection, cb) ->
            collectionName = collection.identity

            afterwards = () =>
                db = connections[collectionName].db
                _schema = {}

                # Grab current auto-increment value from collection's schema definition
                db.describeSchema(collectionName)
                    .then(
                        (schema) ->
                            if schema?
                                deferred schema
                            else
                                # if no schema is defined for this collection
                                # just define an initial AI value for it
                                deferred {autoIncrement: 1}
                    )
                    .then(
                        # Check that the resurrected auto-increment value is valid
                        (schema) ->
                            _schema = schema
                            db.get(collectionName, schema.autoIncrement)
                                .then(
                                    (model) ->
                                        if model?
                                            # That model already exists - look for the max AI value
                                            db.getMaxIndex collectionName
                                        else
                                            deferred -1
                                )
                    )
                    .then(
                        (maxIndex) =>
                            if maxIndex isnt -1
                                # Generate the next-best possible auto-increment key
                                _schema.autoIncrement = maxIndex + 1
                                promisify(@define)(collectionName, _schema)
                            else
                                deferred null
                    )
                    .end(
                        ->
                            cb()
                        ,
                        (err) ->
                            cb err
                    )

            collectionName = collection.identity;

            # If the configuration in this collection corresponds
            # with an existing connection, reuse it
            foundConnection = _.find connections, (connection) ->
                connection && (connection.db.tag is collection.dbTag)

            if foundConnection?
                connections[collection.identity] = foundConnection
                afterwards()

            # Otherwise initialize for the first time
            else
                connect collection, (err, connection) ->
                    # Save reference to connection
                    connections[collection.identity] = connection
                afterwards()


        # The following methods are optional
        ###########################################################

        # Flush data to disk before the adapter shuts down
        teardown: (cb) ->
            # Noting to do here - we always persist data.
            # We keep nothing in memory.
            cb()


        # Create a new collection
        define: (collectionName, definition, cb) ->
            db = connections[collectionName].db

            definition = _.extend {
                # Reset autoIncrement counter
                autoIncrement: 1
            }, definition

            options = {}
            # Create a search index for the commit-log collection
            options.search = collectionName is @commitLog.identity
            # Write schema objects
            db.defineSchema(collectionName, definition, options)
                .end(
                    ->
                        cb()
                    ,
                    (err) ->
                        cb err
                )

        # Fetch the schema for a collection
        # (contains attributes and autoIncrement value)
        describe: (collectionName, cb) ->
            db = connections[collectionName].db

            db.describeSchema(collectionName)
                .end(
                    (schema) ->
                        cb null, schema?.attributes
                    ,
                    (err) ->
                        if err.statusCode is 404
                            # schema not found - maybe it was not defined first.
                            cb()
                        else
                            # something bad happened - escalate the error
                            cb null, err
                )


        # Drop an existing collection
        drop: (collectionName, cb) ->
            db = connections[collectionName].db
            # Delete all models in collection
            db.deleteAll(collectionName)
                .then(
                    ->
                        # Delete the collection schema
                        db.deleteSchema(collectionName)
                )
                .end(
                    ->
                        cb()
                    ,
                    (err) ->
                        cb err
                )


        # Optional override of built-in alter logic
        # Can be simulated with describe(), define(), and drop(),
        # but will probably be made much more efficient by an override here
        # alter: (collectionName, attributes, cb) ->
        #     # Modify the schema of a table or collection in the data store
        #     cb();


        # Create one or more new models in the collection
        create: (collectionName, values, cb) ->
            db = connections[collectionName].db
            values = _.clone(values) || {}

            promisify((cb) =>
                # We are dealing with a commit-log entry, and we need to keep track of the
                # callback functions associated with the lock (these cannot be persisted in
                # the Riak data store).
                if collectionName is @commitLog.identity
                    @saveLockMethods(values)
                        .end(
                            (values) ->
                                cb null, values
                            ,
                            (err) ->
                                cb err
                        )
                else
                    cb null, values
            )()
            .then(
                (values) =>
                    db.create(collectionName, values)
                        .then(
                            (model) =>
                                if collectionName is @commitLog.identity
                                    # We are dealing with a commit-log entry
                                    # re-attatch the callback methods associated to the lock
                                    @restoreLockMethods model
                                else
                                    deferred model
                        )
            )
            .end(
                (model) ->
                    cb null, model
                ,
                (err) ->
                    cb err
            )


        # Find one or more models from the collection
        # using where, limit, skip, and order
        # In where: handle `or`, `and`, and `like` queries
        find: (collectionName, options, cb) ->
            db = connections[collectionName].db

            db.getAllModels(collectionName)
                .then(
                    (models) =>
                        # Get indices from original data which match, in order
                        matchIndices = getMatchIndices models, options

                        resultSet = []
                        for matchIndex in matchIndices
                            resultSet.push _.clone(models[matchIndex])

                        if collectionName is @commitLog.identity
                            # We are dealing with a commit-log entry
                            # re-attatch the callback methods associated to the lock
                            deferred.map(resultSet,
                                (model) =>
                                    @restoreLockMethods model
                            )
                        else
                            deferred resultSet
                )
                .end(
                    (models) ->
                        cb null, models
                    ,
                    (err) ->
                        cb err
                )


        # Update one or more models in the collection
        update: (collectionName, options, values, cb) ->
            db = connections[collectionName].db

            @getAutoIncrementAttribute collectionName,
                (err, aiAttr) ->
                    if err?
                        cb err
                    else
                        db.getAllModels(collectionName)
                            .then(
                                (models) ->
                                    # Query result set using options
                                    matchIndices = getMatchIndices models, options

                                    # Update model(s)
                                    for matchIndex in matchIndices
                                        models[matchIndex] = _.extend models[matchIndex], values

                                    # Replace data collection and go back
                                    deferred.map(matchIndices,
                                        (matchIndex) ->
                                            db.save(collectionName, models[matchIndex][aiAttr], models[matchIndex])
                                    )
                            )
                            .end(
                                (savedModels) ->
                                    cb null, savedModels
                                ,
                                (err) ->
                                    cb err
                            )


        # Delete one or more models from the collection
        destroy: (collectionName, options, cb) ->
            db = connections[collectionName].db

            @getAutoIncrementAttribute collectionName,
                (err, aiAttr) ->
                    if err?
                        cb err
                    else
                        db.getAllModels(collectionName)
                            .then(
                                (models) ->
                                    # Query result set using options
                                    matchIndices = getMatchIndices models, options

                                    # Replace data collection and go back
                                    deferred.map(matchIndices,
                                        (matchIndex) ->
                                            db.remove(collectionName, models[matchIndex][aiAttr])
                                    )
                            )
                            .end(
                                ->
                                    cb null
                                ,
                                (err) ->
                                    cb err
                            )


        # Stream models from the collection
        # using where, limit, skip, and order
        # In where: handle `or`, `and`, and `like` queries
        stream: (collectionName, options, stream) ->
            db = connections[collectionName].db

            db.getAllModels(collectionName)
                .end(
                    (models) ->
                        # Get indices from original data which match, in order
                        matchIndices = getMatchIndices models, options

                        # Write out the stream
                        for matchIndex in matchIndices
                            stream.write _.clone(models[matchIndex])

                        # Finish stream
                        stream.end()
                    ,
                    ->
                        # Finish stream
                        stream.end()
                )


        ############################################################
        # Optional overrides
        ############################################################
        transaction: (transactionName, atomicLogic, afterUnlock) ->
            # Find the oldest lock with the same transaction name
            getNextLock = (locks, currentLock) ->
                nextLock = null
                minId = MAX_INTEGER

                for lock in locks
                    # Ignore locks with different transaction names
                    continue if lock.name isnt currentLock.name

                    # Ignore current lock
                    continue if lock.uuid is currentLock.uuid

                    #Find the lock with the smallest id
                    minId = nextLock.id if nextLock?
                    nextLock = lock if lock.id < minId

                nextLock


            acquireLock = (newLock) ->
                warningTimer = setTimeout(
                    ->
                        console.error "Transaction :: #{newLock.name} is taking an abnormally long time (> #{TRANSACTION_WARNING_TIMEOUT} ms)"
                    , TRANSACTION_WARNING_TIMEOUT
                )

                newLock.atomicLogic null, ->
                    clearTimeout warningTimer
                    releaseLock newLock, arguments

            releaseLock = (currentLock, afterUnlockArgs) =>
                cb = currentLock.afterUnlock
                nextInLine = null

                db.search(collectionName, "name:#{transactionName}")
                    .then(
                        (searchResults) =>
                            nextInLine = getNextLock(_.map(searchResults, (searchResult) -> searchResult.fields), currentLock)
                            if nextInLine?
                                @restoreLockMethods nextInLine
                            else
                                deferred null
                    )
                    .then(
                        (restoredLockObject) ->
                            nextInLine = restoredLockObject
                            db.remove collectionName, currentLock.id
                    )
                    .end(
                        ->
                            # Trigger unlock's callback if specified
                            # > NOTE: do this before triggering the next queued transaction
                            # to prevent transactions from monopolizing the event loop
                            cb?.apply null, afterUnlockArgs

                            acquireLock nextInLine if nextInLine?
                        ,
                        (err) ->
                            cb?(err)
                    )

            collectionName = @commitLog.identity
            db = connections[collectionName].db

            # Generate unique lock
            newLock =
                uuid: uuid.v4(),
                name: transactionName,
                atomicLogic: atomicLogic,
                afterUnlock: afterUnlock

            (promisify(@create)(collectionName, newLock))
                .then(
                    (createdLock) ->
                        newLock = createdLock
                        db.search collectionName, "name:#{transactionName}"
                )
                .end(
                    (searchResults) ->
                        conflict = null
                        for searchResult in searchResults
                            if searchResult.fields.uuid != newLock.uuid and searchResult.id < newLock.id
                                conflict = _.extend { id: searchResult.id }, searchResult.fields
                                break

                        # If there are no conflicts, the lock is acquired!
                        acquireLock newLock unless conflict?

                        # Otherwise, get in line: a lock was acquired before mine, do nothing
                    ,
                    (err) ->
                        return atomicLogic err, ->
                            throw err
                )

        # Optional override of built-in batch create logic for increased efficiency
        # otherwise, uses create()
        #createEach: (collectionName, valuesList, cb) ->
        #    cb()

        # Optional override of built-in findOrCreate logic for increased efficiency
        # otherwise, uses find() and create()
        #findOrCreate: function (collectionName, cb) { cb(); },

        # Optional override of built-in batch findOrCreate logic for increased efficiency
        # otherwise, uses findOrCreate()
        #findOrCreateEach: function (collectionName, cb) { cb(); }

        ############################################################
        # Custom methods
        ############################################################
        saveLockMethods: promisify (model, cb) ->
            if model.uuid?
                @commitLog.funcTable[model.uuid] = {}
                for own attrName of model
                    if _.isFunction(model[attrName])
                        @commitLog.funcTable[model.uuid][attrName] = model[attrName]
                cb null, model
            else
                cb new Error("Commit-log model must have the UUID property defined.")


        restoreLockMethods: promisify (model, cb) ->
            # We are dealing with a commit-log entry
            # re-attatch the callback methods associated to the lock
            if !model.uuid?
                return cb new Error("Commit-log model must have the UUID property defined.")

            if !@commitLog.funcTable[model.uuid]?
                return cb new Error("Cannot find callbacks for commit-log entry with UUID: #{model.uuid}")

            for own funcName of @commitLog.funcTable[model.uuid]
                model[funcName] = @commitLog.funcTable[model.uuid][funcName]
            cb null, model

        ##########################################################################################
        #
        # > NOTE:  There are a few gotchas here you should be aware of.
        #
        #    + The collectionName argument is always prepended as the first argument.
        #      This is so you can know which model is requesting the adapter.
        #
        #    + All adapter functions are asynchronous, even the completely custom ones,
        #      and they must always include a callback as the final argument.
        #      The first argument of callbacks is always an error object.
        #      For some core methods, Sails.js will add support for .done()/promise usage.
        #
        ##########################################################################################

    ##############                 ##########################################
    ############## Private Methods ##########################################
    ##############                 ##########################################
    connect = (collection, cb) ->
        cb null, { db: new RiakDB collection }

    return adapter