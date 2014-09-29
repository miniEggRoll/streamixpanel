batch           = require "#{__dirname}/batch"
parse           = require "#{__dirname}/parser"
splitLine       = require "#{__dirname}/splitLine"
insertAll       = require "#{__dirname}/insertAll"
jwt       = require "#{__dirname}/jwt"
config          = require "#{__dirname}/../config"
{raw}           = require('mixpanel_client') config
_               = require 'underscore'


dumpOption = 
    from_date: new Date('2014-09-1')
    to_date: new Date('2014-09-12')
jwt()
.then ({access_token})->
    raw dumpOption
    .pipe new splitLine()
    .pipe new parse()
    .pipe new batch(100)
    .pipe new insertAll({projectId: 'fourth-gearing-708', datasetId: 'mixpanel', tableId: 'event'}, access_token)
