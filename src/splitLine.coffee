{Transform}     = require 'stream'
_               = require 'underscore'
{delay}         = require "#{__dirname}/../config"

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
        setTimeout done, delay
    _flush: (done)->
        @push @_cache
        @_cache = ''
        do done

module.exports = splitLine
