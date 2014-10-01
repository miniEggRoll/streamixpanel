parse           = require "#{__dirname}/storageParser"
splitLine       = require "#{__dirname}/splitLine"

debug           = require('debug') 'archive'
gcloud          = require 'gcloud'
_               = require 'underscore'
config          = require "#{__dirname}/../config"
mpClient        = require('mixpanel_client')
{from_date, to_date, batchSize, projectId, datasetId, tableId, coreMin} = config

dumpOption = 
    from_date: new Date(from_date)
    to_date: new Date(to_date)

project = gcloud {
    credentials: config.credentials
    projectId: projectId
}

bucket = project.storage.bucket {
    bucketName: 'mixpanel_events'
}

_.each config.mixpanel, (setting, platform)->
    {raw} = mpClient setting
    source = raw dumpOption
    .pipe new splitLine()
    .pipe new parse()
    .pipe bucket.createWriteStream platform
    .on 'error', (err)->
        debug err
    .on 'complete', (fileStat)->
        debug fileStat
