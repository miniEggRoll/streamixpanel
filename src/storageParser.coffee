{Transform}     = require 'stream'
crypto          = require 'crypto'
debug           = require('debug') 'parse'

class parse extends Transform
    constructor: ->
        super
        @_writableState.objectMode = true
        @_readableState.objectMode = true
        @counter = 0
        @errCounter = 0
    _transform: (chunk, encoding, done)->
        try
            str = chunk.toString()
            debug str
            obj = JSON.parse str
            
            md5 = crypto.createHash 'md5'
            md5.update new Buffer(str).toString 'binary'
            id = md5.digest 'hex'
            debug id

            formated =
                event: obj['event']
                cdate: obj.properties.time
                distinct_id: obj.properties.distinct_id
                json: JSON.stringify obj.properties

            @push "#{JSON.stringify formated}\n" 
            @counter ++
        catch e
            debug e
            @errCounter++
        do done
    _flush: (done)->
        debug "success: #{@counter}\nfail:#{@errCounter}"
        @counter = 0
        @errCounter = 0
        do done

module.exports = parse
