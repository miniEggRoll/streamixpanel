fs                      = require 'fs'
debug                   = require('debug') 'insert'
request                 = require 'request'
_                       = require 'underscore'
jwt                     = require "#{__dirname}/jwt"
{tokenUpdatePeriod}     = require "#{__dirname}/../config"

class insertAll
    constructor: ({@datasetId, @projectId, @tableId}, @access_token)->
        insertAll::cdate ?= new Date()
        @_cache = ''
        @timeoutID = setTimeout @updateToken, tokenUpdatePeriod
    updateToken: =>
        delete @timeoutID
        debug 'updating token'
        jwt().then ({@access_token})=>
            debug "token #{@access_token}"
            @timeoutID = setTimeout @updateToken, tokenUpdatePeriod
    exit: ->
        clearTimeout @timeoutID if @timeoutID?
    start: (rows, done)->
        kind = 'event'
        reqOpt =
            method: 'POST'
            url: "https://www.googleapis.com/bigquery/v2/projects/#{@projectId}/datasets/#{@datasetId}/tables/#{@tableId}/insertAll"
            qs: 
                access_token:
                    @access_token
            body: {kind, rows}
            json: true

        request reqOpt, (err, msg, body)->
            if err or (msg.statusCode != 200)
                insertIds = _.pluck(rows, 'insertId').join ','
                output = "#{msg.statusCode}, #{err}, #{body.error?.message}, [#{insertIds}]\n"
                fs.appendFile "err/#{insertAll::cdate}.txt", output, ->
                    debug msg.statusCode, err, body
            else debug msg.statusCode
            done?()

module.exports = insertAll
