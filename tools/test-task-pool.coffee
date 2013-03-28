deferred = require 'deferred'
promisify = deferred.promisify

TaskPool = require '../lib/task-pool'

createTask = () ->
    promisify (data, cb) ->
        setTimeout(
            ()->
                console.log "Doing work: #{data}"
                cb null, data
            ,1000)

taskPool = new TaskPool

taskPool.on 'error', (taskId, err) ->
    console.log "Task #{taskId} has failed. Error: #{err}"

taskPool.on 'task:start', (taskId) ->
    console.log "Task #{taskId} has started."

taskPool.on 'task:complete', (taskId, result) ->
    console.log "Task #{taskId} is now complete with result: #{result}."

ids = for i in [0...10]
    taskPool.addTask createTask(), i, @
    taskPool.drain()

console.log "Created #{ids.length} tasks."

