{Transform}     = require 'stream'
debug           = require('debug') 'parse'

class reformat extends Transform
    constructor: (@project)->
        super
        @_writableState.objectMode = true
        @_readableState.objectMode = true
        @counter = 0
        @errCounter = 0
    _transform: (chunk, encoding, done)->
        str = chunk.toString()
        
        try
            obj = JSON.parse str
        catch e
            @errCounter++
            do done
            return
        obj.project = @project
        @push "#{JSON.stringify obj}\n" 
        @counter++
        debug "#{@project} #{@counter} parsed" unless @counter%10000
        do done
    _flush: (done)->
        debug "#{@project} success: #{@counter}; fail:#{@errCounter}"
        @counter = 0
        @errCounter = 0
        do done

module.exports = reformat
