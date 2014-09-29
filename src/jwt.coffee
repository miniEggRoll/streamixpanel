Q              = require 'q'
querystring    = require 'querystring'
jwt            = require 'jsonwebtoken'
request        = require 'request'
fs             = require 'fs'
_              = require 'underscore'
{Writable}     = require 'stream'

module.exports = ->
    key = fs.readFileSync "#{__dirname}/../key.pem"

    payload = 
        scope: 'https://www.googleapis.com/auth/bigquery'
        iat: new Date().valueOf()/1000
    options = 
        algorithm: 'RS256'
        issuer: '923686992071-u1v5h73ft44847lvdjo6s29rb0ldp7jg@developer.gserviceaccount.com'
        audience: 'https://accounts.google.com/o/oauth2/token'
        expiresInMinutes: '2'

    a = jwt.sign payload, key, options

    params = 
        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer'
        assertion: a

    reqOpt =
        headers: 
            'Content-type': 'application/x-www-form-urlencoded'
        url: 'https://accounts.google.com/o/oauth2/token'
        method: 'POST'
        body: querystring.stringify params

    Q.Promise (resolve, reject, notify)->
        request reqOpt, (err, msg, body)->
            if err then reject err else resolve JSON.parse body
