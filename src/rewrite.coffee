reformat        = require "#{__dirname}/reformat"
splitLine       = require "#{__dirname}/splitLine"
_               = require 'underscore'
gcloud          = require 'gcloud'
cluster         = require "cluster"
debug           = require('debug') 'rewrite'

config          = require "#{__dirname}/../config"
{raw}           = require('mixpanel_client') config
{from_date, to_date, batchSize, projectId, datasetId, coreMin, bucketName, credentials} = config
maxWorker       = Math.min(require('os').cpus().length - 1, coreMin)
tableId = 'events'

project = gcloud {credentials, projectId}
bucket = project.storage.bucket {bucketName}
    
if cluster.isMaster    
    bucket.list (err, files, nextQuery)->
        if err then console.error err
        else 
            _.chain files
            .pluck 'name'
            .each (name)-> 
                worker = cluster.fork()
                worker.on 'message', ({type})->
                    switch type
                        when 'job'
                            worker.send {name, type}
                        when 'complete'
                            debug 'finished processing, killing worker %s', worker.process.pid
                            do worker.kill
                        when 'retry'
                            debug '%s fails, worker %s retries', name, worker.process.pid
                            worker.send {name, type}
                        when 'fail'
                            debug '%s fails, killing worker %s', name, worker.process.pid
                            do worker.kill
                        
    cluster.on 'exit', (worker, code, signal) ->
        debug 'worker %s died', worker.process.pid
                
else
    tried = false
    process.on 'message', ({type, name})->
        switch type
            when 'job'
                bucket.createReadStream name
                .pipe new splitLine()
                .pipe new reformat(name)
                .pipe bucket.createWriteStream "_#{name}"
                .on 'error', (err)->
                    debug err
                    if tried then type = 'fail' else type = 'retry'
                    process.send {type}
                .on 'complete', ({size, id})->
                    debug "done with #{id}, #{size} bytes"
                    process.send {type: 'complete'}

    process.send {type: 'job'}
