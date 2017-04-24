var mongoose = require('mongoose'),
    _ = require('lodash'),
    database = null;

mongoose.Promise = global.Promise; // Removes promise warning (https://github.com/Automattic/mongoose/issues/4291)

exports.connect = function (config, callback) {
    var dbUrl = _buildConnectionString(config);
    var dbOption = _buildConnectionOption(config);
    if (mongoose.connection.readyState != 1) {
        database = mongoose.connect(dbUrl, dbOption, callback);
        database.connection.on('open', function () {
            console.log('Connection to database opened.');
        });
    } else {
        callback();
    }
};

exports.disconnect = function (callback) {
    console.log('Disconnecting from database.');
    database.disconnect(callback);
}

function _buildConnectionString(dbConfig) {
    var serverString = 'mongodb://';
    if (!_.isEmpty(dbConfig.username))
        serverString += dbConfig.username + ':' + dbConfig.password + '@';

    dbConfig.servers.forEach(function (server) {
        serverString += server.host + ':' + server.port + ',';
    });

    return serverString.substring(0, serverString.length - 1) + '/' + dbConfig.database_name + '?ssl=' + dbConfig.ssl;
}

function _buildConnectionOption(database) {
    /*
     * Mongoose by default sets the auto_reconnect option to true.
     * We recommend setting socket options at both the server and replica set level.
     * We recommend a 30 second connection timeout because it allows for
     * plenty of time in most operating environments.
     */
    var options = {
        server: { socketOptions: { keepAlive: 1, connectTimeoutMS: 10 * 60 * 1000 } },
        replset: { rs_name: database.replicaSetName, socketOptions: { keepAlive: 1, connectTimeoutMS: 10 * 60 * 1000 } },
        promiseLibrary: global.Promise
    };

    if (!_.isEmpty(database.username)) {
        options.auth = { authSource: 'admin' };
    }

    return options;
}
