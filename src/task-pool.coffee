EventEmitter = require('events').EventEmitter
uuid = require 'node-uuid'
_ = require 'underscore'

module.exports = class TaskPool extends EventEmitter
    name: "TaskPool"

    constructor: ->
        taskQueue = []

        working = false

        @addTask = (promise, args, ctx) ->
            args = [args] if !(_.isArray args)
            newTask =
                promise: promise
                args: args
                ctx: ctx
                id: uuid.v4()

            taskQueue.push newTask
            newTask.id


        runNextTask = =>
            currentTask = taskQueue.shift()

            if !currentTask?
                @emit 'drain:complete'
                working = false
                return null

            @emit 'task:start', currentTask.id
            currentTask.promise.apply(currentTask.ctx, currentTask.args)
                .end(
                    (result) =>
                        @emit 'task:complete', currentTask.id, result
                        runNextTask()
                ,
                (err) =>
                    @emit 'error', currentTask.id, err
                    runNextTask()
                )

        @drain = ->
            return if working
            working = true
            runNextTask()

        @getTaskCount = ->
            taskQueue.length

