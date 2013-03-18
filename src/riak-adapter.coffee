db = require('riak-js').getClient()
_ = require 'underscore'


deferred = require 'deferred'
promisify = deferred.promisify

dbRemovePromise = _.bind promisify(db.remove), db
dbSavePromise = _.bind promisify(db.save), db
dbKeyCountPromise = _.bind promisify(db.count), db


class RiakAdapter
    setModelKey: promisify (bucket, model, cb) ->
        if model.id?
            cb null, model
        else
            dbKeyCountPromise(bucket)
                .then(
                    (results) ->
                        model.id = results.shift() + 1
                        cb null, model
                )


    create: (collectionName, values, cb) ->
        bucket = collectionName
        model = values

        @setModelKey(bucket, model)
            .then(
                (model) ->
                    dbSavePromise(bucket, model.id, model, {returnbody: true})
                        .then(
                            (result) ->
                                cb null, result.shift()

                            (err) ->
                                cb err
                        )
                ,
                (err) ->
                    cb err
            )
            .end()



    find: (collectionName, options, cb) ->
        if options?.sort?
            console.log "Sorting by: #{JSON.stringify options.sort, null, ''}"

        if options?.where?
            console.log "Filter by: #{JSON.stringify options.where, null, ''}"


        cb new Error("NOT_IMPLEMENTED")



#dbAdapter = new RiakAdapter
#
#model =
#    'name':'andrei'
#    'email':'andrei.fecioru@gmail.com'
#
#dbAdapter.create 'user', model
#                 , (err, model) ->
#                     if err?
#                         console.log "ERROR: #{JSON.stringify(err, null, '  ')}"
#                     else
#                         console.log "SUCCESS: #{JSON.stringify(model, null, '  ')}"

module.exports = new RiakAdapter

