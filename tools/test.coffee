deferred = require 'deferred'
promisify = deferred.promisify

RiakDB = require '../lib/riak-db'

riakDB = new RiakDB('test')

riakDB.defineSchema('user', { 'value': 1 })
    .then(
        (definition) ->
            console.log "Schema defined: #{JSON.stringify definition, null, ''}"
            promisify(riakDB.describeSchema('user'))
    )
    .then(
        (definition) ->
            console.log "Schema description: #{JSON.stringify definition, null, ''}"
    )
    .end()