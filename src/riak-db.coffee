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

    get: promisify (collectionName, cb) ->
        unless collectionName?
            cb new Error("RiakDB#get - Illegal argument.")
        else
            keyStream = db.keys collectionName, { keys: 'stream' }, undefined
            keyList = []

            keyStream.on 'keys', (keys) ->
                for key in keys
                    keyList.push key

            keyStream.on 'end', () ->
                cb null, keyList

            keyStream.start()

    defineSchema: promisify (collectionName, definition, cb) ->
        unless (collectionName? && definition?)
            cb new Error("RiakDB#defineSchema - Illegal argument.")
        else
            dbSavePromise("#{@tag}_schema", collectionName, definition, { returnbody: true })
                .then(
                    (result) ->
                        cb null, result.shift()
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




