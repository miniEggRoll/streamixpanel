Q               = require 'q'
{Writable}      = require 'stream'
debug           = require('debug') 'resolve'

class resolve extends Writable
    constructor: ->
        super
        @_writableState.objectMode = true
    _write: (chunk, encoding, done)->
        chunk.then ({msg, body})->
            if body? and body.insertErrors? then debug body.insertErrors[0].errors[0]
            debug msg.statusCode, rows.length
        do done

module.exports = resolve
