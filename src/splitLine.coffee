{Transform}     = require 'stream'
_               = require 'underscore'

class splitLine extends Transform
    constructor: ->
        super
        @_cache = ''
        @_readableState.objectMode = true
    _transform: (chunk, encoding, done)->
        @_cache += chunk.toString()
        [lines..., @_cache] = @_cache.split '\n'

        _.chain lines
        .compact()
        .each @push.bind @
        .value()
        do done
    _flush: (done)->
        @push @_cache
        @_cache = ''
        do done

module.exports = splitLine
