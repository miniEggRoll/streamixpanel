{Transform}      = require 'stream'
debug           = require('debug') 'resolve'

class resolve extends Transform
    constructor: ->
        super
        @_writableState.objectMode = true
        @errCount = 0
        @successCount = 0
    _transform: (chunk, encoding, done)->
        chunk.then ({msg, body})->
            if body? and body.insertErrors? 
                debug body.insertErrors[0].errors[0]
                @errCount++
                debug @errCount if (@errCount % 10) is 0
            else @successCount++
            debug msg.statusCode
        do done
    _flush: (done)->
        debug "#{@successCount} batch inserted, #{@errCount} errors"
        @errCount = 0
        @successCount = 0
        do done

module.exports = resolve
