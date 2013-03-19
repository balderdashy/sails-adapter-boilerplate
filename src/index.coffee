############################################################
# :: sails-riak
# -> adapter
############################################################

deferred = require 'deferred'
async = require 'async'
_ = require 'underscore'
_.str = require 'underscore.string'

RiakDB = require './riak-db'

module.exports = do () ->
    # Load criteria module
    getMatchIndices = require'./criteria.js'

    # Maintain a list of active DB objects
    connections = { }

    adapter =
        # Whether this adapter is syncable (yes)
        syncable: true


        # Enable transactions by allowing Riak to create a commitLog
        commitLog:
             identity: 'commit_log'
             adapter: 'sails-riak'


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
                connections[collectionName].db.describeSchema(collectionName)
                    .then(
                        (schema) ->
                            console.log "Schema description: #{JSON.stringify schema, null, ''}"
                            cb null, schema.attributes
                    )

                @getAutoIncrementAttribute collectionName, (err, aiAttr) ->
                    console.log "AUTO-INCREMENT attr: #{JSON.stringify arguments, null, '  '}"

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
                    (schema) ->
                        console.log "Schema defined: #{JSON.stringify schema, null, ''}"
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
                        console.log "Schema description: #{JSON.stringify schema, null, ''}"
                        cb null, schema?.attributes
                    ,
                    (err) ->
                        cb err
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


        # REQUIRED method if users expect to call Model.create() or any methods
        create: (collectionName, values, cb) ->
            # Create a single new model specified by values
            dbAdapter.create collectionName, values, cb


        # Find one or more models from the collection
        # using where, limit, skip, and order
        # In where: handle `or`, `and`, and `like` queries
        find: (collectionName, options, cb) ->
            data = connections[collectionName].db.get(collectionName) # TODO: from here


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
        cb null, { db: new RiakDB(collection.logTag) }

    return adapter