parse           = require "#{__dirname}/storageParser"
splitLine       = require "#{__dirname}/splitLine"

debug           = require('debug') 'archive'
gcloud          = require 'gcloud'
_               = require 'underscore'
cluster         = require 'cluster'
config          = require "#{__dirname}/../config"
mpClient        = require('mixpanel_client')
{from_date, to_date, projectId, bucketName, credentials} = config

if cluster.isMaster
    cluster.on 'exit', (worker, code, signal) ->
        debug 'worker %s died', worker.process.pid

    _.each config.mixpanel, (setting, platform)->
        worker = cluster.fork()
        worker.on 'message', ({type})->
            switch type
                when 'job'
                    worker.send {setting, platform, type}
                when 'complete'
                    debug 'finished processing, killing worker %s', worker.process.pid
                    do worker.kill
                when 'retry'
                    debug '%s fails, worker %s retries', platform, worker.process.pid
                    worker.send {setting, platform, type}
                when 'fail'
                    debug '%s fails, killing worker %s', platform, worker.process.pid
                    do worker.kill
else
    dumpOption = 
        from_date: new Date(from_date)
        to_date: new Date(to_date)

    project = gcloud {credentials, projectId}
    bucket = project.storage.bucket {bucketName}
    tried = false
    process.on 'message', ({setting, platform, type})->
        switch type
            when 'job'
                debug 'start archiving %s', platform
                {raw} = mpClient setting
                raw dumpOption
                .pipe new splitLine()
                .pipe new parse(platform)
                .pipe bucket.createWriteStream "#{platform}_#{from_date}_#{to_date}"
                .on 'error', (err)->
                    debug err
                    if tried then type = 'fail' else type = 'retry'
                    process.send {type}
                .on 'complete', ({size, id})->
                    debug "done with #{id}, #{size} bytes"
                    process.send {type: 'complete'}

    process.send {type: 'job'}



