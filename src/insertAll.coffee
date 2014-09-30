Q                       = require 'q'
debug                   = require('debug') 'insert'
{Transform}             = require 'stream'
qs                      = require 'querystring'
request                 = require 'request'
_                       = require 'underscore'
jwt                     = require "#{__dirname}/jwt"
{tokenUpdatePeriod}     = require "#{__dirname}/../config"

class insertAll extends Transform
    constructor: ({@datasetId, @projectId, @tableId}, @access_token)->
        super
        @_cache = ''
        @_writableState.objectMode = true
        @_readableState.objectMode = true
        @_timeoutID = setTimeout @updateToken, tokenUpdatePeriod
    updateToken: =>
        debug 'updating token'
        jwt().then ({@access_token})=>
            debug "token #{@access_token}"
            @_timeoutID = setTimeout @updateToken, tokenUpdatePeriod
    _transform: (rows, encoding, done)->
        kind = 'event'
        reqOpt =
            method: 'POST'
            url: "https://www.googleapis.com/bigquery/v2/projects/#{@projectId}/datasets/#{@datasetId}/tables/#{@tableId}/insertAll"
            qs: 
                access_token:
                    @access_token
            body: {kind, rows}
            json: true

        promise =  Q.Promise (resolve, reject, notify)->
            request reqOpt, (err, msg, body)->
                if err then reject err else resolve {msg, body}
        
        @push promise
        do done
    _flush: (done)->
        clearTimeout @_timeoutID

module.exports = insertAll
