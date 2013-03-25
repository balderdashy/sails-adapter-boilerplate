############################################################
# :: sails-riak
# -> adapter
############################################################

deferred = require 'deferred'
promisify = deferred.promisify

_ = require 'underscore'
_.str = require 'underscore.string'
_.mixin _.str.exports()

async = require 'async'
asyncSeriesPromise = promisify async.series

RiakDB = require './riak-db'

module.exports = do () ->
    # Load criteria module
    getMatchIndices = require './criteria.js'

    # Maintain a list of active DB objects
    connections = { }

    adapter =
        # Whether this adapter is syncable (yes)
        syncable: true


        # Enable transactions by allowing Riak to create a commitLog
        commitLog:
             identity: 'commit_log'
             adapter: 'sails-riak'
             dbTag: 'commit_log'


        # Default configuration for collections
        # (same effect as if these properties were included at the top level of the model definitions)
        defaults:
            port: 8098
            host: 'localhost'
            migrate: 'drop'

            # Bucket namespace: a prefix pre-pended to all Riak buckets
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
                        (maxIndex) ->
                            if maxIndex isnt -1
                                # Generate the next-best possible auto-increment key
                                _schema.autoIncrement = maxIndex + 1
                                db.defineSchema collectionName, _schema
                            else
                                deferred null
                    )
                    .end(
                        (updatedSchema) ->
                            if updatedSchema?
                                console.log "Updated stale auto-increment for collection: #{collectionName}"
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
            definition = _.extend {
                # Reset autoIncrement counter
                autoIncrement: 1
            }, definition

            # Write schema objects
            connections[collectionName].db.defineSchema(collectionName, definition)
                .then(
                    () ->
                        cb()
                    ,
                    (err) ->
                        cb err
                )

        # Fetch the schema for a collection
        # (contains attributes and autoIncrement value)
        describe: (collectionName, cb) ->
            connections[collectionName].db.describeSchema(collectionName)
                .then(
                    (schema) ->
                        cb null, schema?.attributes
                    ,
                    (err) ->
                        if err.statusCode is 404
                            # schema not found - maybe it was not defined first.
                            cb null, null
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
                    (deletedKeys) ->
                        # Delete the collection schema
                        db.deleteSchema(collectionName)
                )
                .end(
                    (schemaKey) ->
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
            values = _.clone(values) || {}
            db = connections[collectionName].db

            if _(values.name).startsWith "parallel_test user"
                console.log "CREATE #{JSON.stringify values}"

            # Lookup collection schema so we know all of the attribute
            # names and the current auto-increment value
            db.describeSchema(collectionName)
                .then(
                    (schema) ->
                        if schema?
                            deferred schema
                        else
                            throw badSchemaError(collectionName, db)
                )
                .then(
                    (schema) ->
                        doAutoIncrement(db, collectionName, schema, values)
                            .then(
                                (data) ->
                                    model = data.values
                                    key = data.schema.autoIncrement
                                    db.save(collectionName, key, model)
                            )
                )
                .end(
                    (model) ->
                        console.log "CREATE - Saved model: #{JSON.stringify model}"
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
                .end(
                    (models) ->
                        # Get indices from original data which match, in order
                        matchIndices = getMatchIndices models, options
                        resultSet = []

                        for matchIndex in matchIndices
                            resultSet.push _.clone(models[matchIndex])

                        cb null, resultSet
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
                                        db.delete(collectionName, models[matchIndex][aiAttr])
                                )
                        )
                        .end(
                            (deletedKeys) ->
                                console.log "DESTROY: Deleted keys: #{JSON.stringify deletedKeys}"
                                cb null
                            ,
                            (err) ->
                                cb err
                        )


        # REQUIRED method if users expect to call Model.stream()
        stream: (collectionName, options, stream) ->
            console.log "STREAMING"
            # options is a standard criteria/options object (like in find)

            # stream.write() and stream.end() should be called.
            # for an example, check out:
            # https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247
            cb Error("NOT_IMPLEMENTED")


        ############################################################
        # Optional overrides
        ############################################################

        # Optional override of built-in batch create logic for increased efficiency
        # otherwise, uses create()
#        createEach: (collectionName, valuesList, cb) ->
#            taskList = []
#            for values in valuesList
#                taskList = (cb) =>
#                    console.log "RUNNING JOB: Values: #{JSON.stringify values}"
#                    @create collectionName, values, cb
#
#            asyncSeriesPromise(taskList)
#                .end(
#                    (models) ->
#                        console.log "CREATE-EACH: Models: #{JSON.stringify models}"
#                        cb null, models
#                    ,
#                    (err) ->
#                        cb err
#                )

        # Optional override of built-in findOrCreate logic for increased efficiency
        # otherwise, uses find() and create()
        #findOrCreate: function (collectionName, cb) { cb(); },

        # Optional override of built-in batch findOrCreate logic for increased efficiency
        # otherwise, uses findOrCreate()
        #findOrCreateEach: function (collectionName, cb) { cb(); }

        ############################################################
        # Custom methods
        ############################################################

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
        cb null, { db: new RiakDB(collection.dbTag) }


    # Look for auto-increment field, increment counter accordingly, and return refined value set
    doAutoIncrement = promisify (db, collectionName, schema, values, cb) ->
        # Determine the attribute names which will be included in the created object
        attrNames = _.keys _.extend({}, schema.attributes, values)

        _.each attrNames, (attrName) ->
            # But only if the given auto-increment value
            # was NOT actually specified in the value set,
            if (_.isObject(schema.attributes[attrName]) && schema.attributes[attrName].autoIncrement)
                if (!values[attrName])
                    # increment AI fields in values set
                    values[attrName] = schema.autoIncrement

                    # update the collection schema with the new AI value
                    schema.autoIncrement += 1
                    db.defineSchema(collectionName, schema)
                        .then(
                            () ->
                                # do nothing
                        ,
                        (err) ->
                            cb err
                        )

        cb null, {schema: schema, values: values}



    badSchemaError = (collectionName, db) ->
        new Error "Cannot get schema for collection: #{collectionName} for DB instance: #{db.tag}"


    return adapter