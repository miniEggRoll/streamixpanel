dispatch        = require "#{__dirname}/dispatch"
batch           = require "#{__dirname}/batch"
parse           = require "#{__dirname}/parser"
splitLine       = require "#{__dirname}/splitLine"
insertAll       = require "#{__dirname}/insertAll"
jwt             = require "#{__dirname}/jwt"
_               = require 'underscore'
cluster         = require "cluster"
debug           = require('debug') 'master'

maxWorker       = require('os').cpus().length - 1
config          = require "#{__dirname}/../config"
{raw}           = require('mixpanel_client') config
{from_date, to_date, batchSize, projectId, datasetId, tableId} = config

if cluster.isMaster
    dumpOption = 
        from_date: new Date(from_date)
        to_date: new Date(to_date)
    
    dp = new dispatch()

    source = raw dumpOption
    .pipe new splitLine()
    .pipe new parse()
    .pipe new batch(batchSize)
    .pipe dp

    cluster.on 'exit', (worker, code, signal) ->
        debug 'worker %s died', worker.process.pid

    _.times maxWorker, ->
        worker = cluster.fork()
        dp.enqueue worker

        worker.on 'message', ({type})->
            switch type
                when 'drain'
                    debug 'worker %s drained', worker.process.pid
                    if source.hasEnded
                        debug 'finished processing, killing worker %s', worker.process.pid
                        do worker.kill
                    else
                        debug 'nothing to read, enqueue'
                        dp.enqueue worker

else
    jwt().then ({access_token})->
        insert = new insertAll({projectId, datasetId, tableId}, access_token)
        process.send {type: 'register'}

        process.on 'message', ({data})->
            insert.start data, -> process.send {type: 'drain'}
