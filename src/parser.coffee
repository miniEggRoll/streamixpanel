{Transform}     = require 'stream'

class parse extends Transform
    constructor: ->
        super
        @_writableState.objectMode = true
        @_readableState.objectMode = true
    _transform: (chunk, encoding, done)->
        try
            obj = JSON.parse chunk.toString()
            @push {
                json:
                    event: obj['event']
                    cdate: obj.properties.time
                    distinct_id: obj.properties.distinct_id
                    json: JSON.stringify obj.properties
            }
        catch e
            # ...
        do done

module.exports = parse
