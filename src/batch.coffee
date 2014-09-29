{Transform} = require 'stream'

class Batch extends Transform
    constructor: (@queue = 30) ->
        super
        @_writableState.objectMode = true
        @_readableState.objectMode = true
        @debug = require('debug')('batch')
        @_cache = []
        @once 'end', =>
            @debug 'ended'

    _flush: (done) =>
        @debug 'flushing'
        @push(@_cache) if @_cache.length > 0
        @_cache = []
        done?()

    _transform: (data, enc, done) =>
        @_cache.push data
        do @_flush if @_cache.length is @queue
        do done

module.exports = Batch
