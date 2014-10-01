parse           = require "#{__dirname}/storageParser"
splitLine       = require "#{__dirname}/splitLine"

debug           = require('debug') 'archive'
gcloud          = require 'gcloud'

config          = require "#{__dirname}/../config"
{raw}           = require('mixpanel_client') config
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

source = raw dumpOption
.pipe new splitLine()
.pipe new parse()
.pipe bucket.createWriteStream tableId
.on 'error', (err)->
    debug err
.on 'complete', (fileStat)->
    debug fileStat
