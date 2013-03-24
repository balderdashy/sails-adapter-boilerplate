############################################################
# :: sails-riak
# -> adapter
############################################################

deferred = require 'deferred'
promisify = deferred.promisify
async = require 'async'
_ = require 'underscore'
_.str = require 'underscore.string'

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

            # Bucket namespace: a prefix pre-pended to all Riak buckets
            dbTag: 'test'


        # This method runs when a model is initially registered at server start time
        registerCollection: (collection, cb) ->
            #console.log "COLLECTION: #{JSON.stringify collection, null, '  '}"
            collectionName = collection.identity

            afterwards = () =>
                db = connections[collectionName].db
                db.describeSchema(collectionName)
                    .then(
                        (schema) ->
                            console.log "AUTO-INCREMENT: #{JSON.stringify schema.autoIncrement}"
                            deferred schema.autoIncrement
                    )
                    .then(
                        (autoIncrement) =>
                            # Check that the resurrected auto-increment value is valid
                            @find collectionName, {
                                    where :
                                        id: autoIncrement
                                }, (err, models) ->
                                    if err?
                                        cb err
                                    else
                                        if models?.length?
                                            db.getMaxIndex(collectionName)
                                                .then(
                                                    (models) ->
                                                        console.log "DONE!!"
                                                )
                    )
                    .end()

                @getAutoIncrementAttribute collectionName, (err, aiAttr) ->
                    # Get the current auto-increment value for this collection
                    # Check that the resurrected auto-increment value is valid

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

            cb()


        # The following methods are optional
        ###########################################################

        # Optional hook fired when a model is unregistered, typically at server halt
        # useful for tearing down remaining open connections, etc.
        teardown: (cb) ->
            cb()


        # Create a new collection
        define: (collectionName, definition, cb) ->
            console.log "DEFINING SCHEMA: #{collectionName}"

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
            console.log "DESCRIBING SCHEMA: #{collectionName}"
            connections[collectionName].db.describeSchema(collectionName)
                .then(
                    (schema) ->
                        #console.log "Schema description: #{JSON.stringify schema, null, ' '}"
                        cb null, schema?.attributes
                    ,
                    (err) ->
                        console.log "Schema description error: #{JSON.stringify err, null, ''}"
                        if err.statusCode is 404
                            # schema not found - maybe it was not defined first.
                            cb null, null
                        else
                            # something bad happened - escalate the error
                            cb null, err
                )


        # REQUIRED method if integrating with a schemaful database
        drop: (collectionName, cb) ->
            # Drop a "table" or "collection" schema from the data store
            cb()


        # Optional override of built-in alter logic
        # Can be simulated with describe(), define(), and drop(),
        # but will probably be made much more efficient by an override here
        # alter: (collectionName, attributes, cb) ->
        #     # Modify the schema of a table or collection in the data store
        #     cb();


        # Create one or more new models in the collection
        create: (collectionName, values, cb) ->
            console.log "CREATE: #{collectionName} => #{JSON.stringify values, null, ''}"

            values = _.clone(values) || {}
            db = connections[collectionName].db

            # Lookup schema & status so we know all of the attribute names and the current auto-increment value
            db.describeSchema(collectionName)
                .then(
                    (schema) ->
                        cb null, schema.attributes
                        deferred schema
                    ,
                    (err) ->
                        console.log "ERR: #{JSON.stringify err, null, '  '}"
                        cb badSchemaError collectionName, db
                )
                .then(
                    (schema) ->
                        doAutoIncrement(collectionName, schema.attributes, values)
                            .then(
                                (values) ->
                                    console.log "Values: #{JSON.stringify values, null, '  '}"
                                ,
                                (err) ->
                                    console.log "Error: #{JSON.stringify err, null, '  '}"
                                    cb err
                            )
                )
                .end()



        # Find one or more models from the collection
        # using where, limit, skip, and order
        # In where: handle `or`, `and`, and `like` queries
        find: (collectionName, options, cb) ->
            console.log "----> FIND: #{collectionName}"

            connections[collectionName].db.getAllModelsInCollection(collectionName)
                .then(
                    (models) ->
                        console.log "<---- FIND: #{models.length}"

                        # Get indices from original data which match, in order
                        matchIndices = getMatchIndices models, options
                        resultSet = []

                        _.each matchIndices, (matchIndex) ->
                            resultSet.push _.clone(models[matchIndex])

                        cb null, resultSet
                    ,
                    (err) ->
                        console.log "FIND: ERROR"
                        cb err
                )


        # REQUIRED method if users expect to call Model.update()
        update: (collectionName, options, values, cb) ->
            # Filter by criteria in options to generate result set

            # Update all model(s) in the result set

            # Respond with error or a *list* of models that were updated
            cb()


        # REQUIRED method if users expect to call Model.destroy()
        destroy: (collectionName, options, cb) ->
            # Filter by criteria in options to generate result set

            # Destroy all model(s) in the result set

            # Return an error or nothing at all
            cb()


        # REQUIRED method if users expect to call Model.stream()
        stream: (collectionName, options, stream) ->
            # options is a standard criteria/options object (like in find)

            # stream.write() and stream.end() should be called.
            # for an example, check out:
            # https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247


        ############################################################
        # Optional overrides
        ############################################################

        # Optional override of built-in batch create logic for increased efficiency
        # otherwise, uses create()
        #createEach: function (collectionName, cb) { cb(); },

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
        console.log "CONNECTING: #{JSON.stringify collection.dbTag, null, '  '}"
        cb null, { db: new RiakDB(collection.dbTag) }


    # Look for auto-increment field, increment counter accordingly, and return refined value set
    doAutoIncrement = promisify (collectionName, attributes, values, cb) ->

        # Determine the attribute names which will be included in the created object
        attrNames = _.keys _.extend({}, attributes, values)

        console.log "ATTR-NAMES: #{JSON.stringify attrNames, null, '  '}"

        _.each attrNames, (attrName) ->
            # But only if the given auto-increment value
            # was NOT actually specified in the value set,
            if (_.isObject(attributes[attrName]) && attributes[attrName].autoIncrement)
                if (!values[attrName])
                    # increment AI fields in values set
                    cb Error("DON'T KNOW WHAT TO DO...")

        cb null, values



    badSchemaError = (collectionName, db) ->
        new Error "Cannot get schema for collection: #{collectionName} using schema prefix: #{db.tag}"


    return adapter