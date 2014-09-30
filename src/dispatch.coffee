{Writable}  = require 'stream'
debug       = require('debug') 'dispatch'

class Dispatch extends Writable
    constructor: ->
        super
        @_writableState.objectMode = true
        @debug = debug
        @queue = []
        @hasEnded = false
        @once 'finish', =>
            @debug 'hasEnded'
            @hasEnded = true
            @queue.forEach (worker) =>
                @debug 'finished processing, killing worker %s', worker.process.pid
                worker.kill()

    enqueue: (worker) =>
        @queue.push worker
        @emit 'enqueue'
        @

    _write: (data, enc, done) =>
        dispatch = =>
            @debug 'dispatch works'
            worker = @queue.pop()
            worker.send {data}
            do done

        if @queue.length > 0
            do dispatch
        else @once 'enqueue', dispatch


module.exports = Dispatch
