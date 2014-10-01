{Transform}     = require 'stream'
crypto          = require 'crypto'
debug           = require('debug') 'parse'

class parse extends Transform
    constructor: (@platform)->
        super
        @_writableState.objectMode = true
        @_readableState.objectMode = true
        @counter = 0
        @errCounter = 0
    _transform: (chunk, encoding, done)->
        str = chunk.toString()
        
        try
            obj = JSON.parse str
            formated =
                event: obj['event']
                cdate: obj.properties?.time
                distinct_id: obj.properties?.distinct_id
                json: JSON.stringify obj.properties
        catch e
            @errCounter++
            do done
            return

        @push "#{JSON.stringify formated}\n" 
        @counter++
        debug "#{@platform} #{@counter} parsed" unless @counter%10000
        do done
    _flush: (done)->
        debug "#{@platform} success: #{@counter}; fail:#{@errCounter}"
        @counter = 0
        @errCounter = 0
        do done

module.exports = parse
