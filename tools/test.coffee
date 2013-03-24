deferred = require 'deferred'
promisify = deferred.promisify

RiakDB = require '../lib/riak-db'

riakDB = new RiakDB('test')

models = [
    {name: "Andrei", id:1}
    {name: "Mihai", id:2}
    {name: "Radu", id:3}
]

riakDB.save('user', models)
    .then(
        (models) ->
            console.log "#{models.length} models saved."
            riakDB.getMaxIndex('user')
    )
    .then(
        (models) ->
            console.log "Models --- : #{JSON.stringify models, null, ''}"
        ,
        (err) ->
            console.log "ERROR: #{JSON.stringify err.message}"
    )
    .end()
