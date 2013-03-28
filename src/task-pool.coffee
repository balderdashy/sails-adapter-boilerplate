EventEmitter = require('events').EventEmitter
uuid = require 'node-uuid'
_ = require 'underscore'

module.exports = class TaskPool extends EventEmitter
    taskQueue: []

    working: false

    addTask: (promise, args, ctx) ->
        args = [args] if !(_.isArray args)
        newTask =
            promise: promise
            args: args
            ctx: ctx
            id: uuid.v4()

        @taskQueue.push newTask
        newTask.id


    drain: () ->
        return null if @working

        while @taskQueue.length
            @working = true
            currentTask = @taskQueue.shift()
            @emit 'task:start', currentTask.id
            currentTask.promise.apply(currentTask.ctx, currentTask.args)
                .end(
                    (result) =>
                        @emit 'task:complete', currentTask.id, result
                    ,
                    (err) =>
                        @emit 'error', currentTask.id, err
                )

        @working = false
        null
