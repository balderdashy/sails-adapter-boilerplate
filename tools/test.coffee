deferred = require 'deferred'
promisify = deferred.promisify

RiakDB = require '../lib/riak-db'

riakDB = new RiakDB('test')

settle_time = 2;

models = [
    {name: "Andrei", id:1}
    {name: "Mihai", id:2}
    {name: "Radu", id:3}
    {name: "Gigi", id:4}
]

riakDB.save('user', models)
    .then(
        (savedModels) ->
            console.log "#{savedModels.length} models saved."
            riakDB.getAll 'user'
    )
    .then(
        (models) ->
            console.log "Models: #{models.length}"
            riakDB.getMaxIndex 'user'
    )
    .then(
        (maxIndex) ->
            console.log "Max index: #{maxIndex}"
            riakDB.get 'user', "5"
    )
    .then(
        (model) ->
            console.log "Model: #{JSON.stringify model}"
            riakDB.getCollections()
    )
    .then(
        (buckets) ->
            console.log "Buckets: #{JSON.stringify buckets}"
        ,
        (err) ->
            console.log "ERROR: #{JSON.stringify err}"

    )
    .end()

