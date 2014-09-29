{Writable} = require 'stream'
qs = require 'querystring'
request = require 'request'
_ = require 'underscore'

class insertAll extends Writable
    constructor: ({@datasetId, @projectId, @tableId}, @access_token)->
        super
        @_cache = ''
        @_writableState.objectMode = true
    _write: (rows, encoding, done)->
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
            if body.insertErrors? then console.log body.insertErrors[0].errors[0]
            do done

module.exports = insertAll
