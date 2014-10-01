{Transform}     = require 'stream'
crypto          = require 'crypto'
debug           = require('debug') 'parse'

class parse extends Transform
    constructor: ->
        super
        @_writableState.objectMode = true
        @_readableState.objectMode = true
    _transform: (chunk, encoding, done)->
        try
            str = chunk.toString()
            debug str
            obj = JSON.parse str
            
            md5 = crypto.createHash 'md5'
            md5.update new Buffer(str).toString 'binary'
            id = md5.digest 'hex'
            debug id

            @push {
                insertId: id
                json:
                    event: obj['event']
                    cdate: obj.properties.time
                    distinct_id: obj.properties.distinct_id
                    json: JSON.stringify obj.properties
            }
        catch e
            debug e
        do done

module.exports = parse
