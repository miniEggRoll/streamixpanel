resolve         = require "#{__dirname}/resolve"
batch           = require "#{__dirname}/batch"
parse           = require "#{__dirname}/parser"
splitLine       = require "#{__dirname}/splitLine"
insertAll       = require "#{__dirname}/insertAll"
jwt             = require "#{__dirname}/jwt"
config          = require "#{__dirname}/../config"
{raw}           = require('mixpanel_client') config
_               = require 'underscore'

{from_date, to_date, batchSize} = config

dumpOption = 
    from_date: new Date(from_date)
    to_date: new Date(to_date)

jwt()
.then ({access_token})->
    raw dumpOption
    .pipe new splitLine()
    .pipe new parse()
    .pipe new batch(batchSize)
    .pipe new insertAll({projectId: 'fourth-gearing-708', datasetId: 'mixpanel', tableId: 'ios'}, access_token)
    .pipe new resolve()
