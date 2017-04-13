'use strict';
const mongoose = require('mongoose');
const async = require('async');
const config = require('config');
const database = require('./db');
const _ = require('lodash');
const Schema = mongoose.Schema;
const no_schema = new Schema({}, { strict: false });
const moment = require('moment-timezone');
const mongodb_config = config.get('mongodb');
const model = mongoose.model('advertinstances', no_schema, 'advertinstances');

module.exports.hello = (event, context, callback) => {
  mongodb_config.database_name = event.database_name;

  console.log('Database: ' + event.database_name
    + '. advert_id: ' + event.advert._id
    + '. locations length ' + event.locations.length);

  const locationIds = _.map(event.locations, function (location) {  // Copy the ids for the parallel tasks
    return mongoose.Schema.ObjectId(location._id);
  });

  async.waterfall([
    function (callback) {
      database.connect(mongodb_config, callback);
    },
    function (callback) {
      var tasks = {
        markAsDelete: function (callback) {
          model.update(
            {
              'advert.id': mongoose.Schema.ObjectId(event.advert._id),
              'locations._id': { $nin: locationIds }
            },
            {
              $set: { 'advert.status': 'deleted' }
            },
            {
              upsert: false,
              multi: true
            },
            function (err, result) {
              if (!err)
                console.log('Advertinstance updated with advert id ' + event.advert._id + '. Found ' + result.n + ' and marked ' + result.nModified + ' as deleted');
              else
                console.log('Error with Advertinstance update. Msg: ' + err.message);
              callback(err, result);
            }
          );
        },
        upsert: function (callback) {
          if (_.isEmpty(locationIds)) {
            return callback(null);
          }

          var chunks = _.chunk(locationIds, 100);
          async.eachSeries(chunks, function (chunk, callback) {
            var advert_copy = _.cloneDeep(event.advert);
            var bulk = model.collection.initializeUnorderedBulkOp();

            _.forEach(event.locations, function (location) {
              var advertinstance = {};
              advertinstance.advert = advert_copy;
              advertinstance.location = location;

              if (advert_copy.start_date && advert_copy.end_date) {
                // Every advert should be valid for issuing and redemption at the same hour:minute everywhere
                // For example: [8:00 AM Sydney, 8:00 AM Tokyo] instead of [8:00 AM Sydney 7:00 AM Tokyo]
                advertinstance.advert.start_date = _convertTime(advert_copy.start_date, location.timezone);
                advertinstance.advert.end_date = _convertTime(advert_copy.end_date, location.timezone);
              }

              if (advert_copy.voucher) {
                advertinstance.advert.voucher.start_date = _convertTime(advert_copy.voucher.start_date, location.timezone);
                advertinstance.advert.voucher.end_date = _convertTime(advert_copy.voucher.end_date, location.timezone);
              }

              var query = {
                'advert._id': mongoose.Schema.ObjectId(advert_copy._id),
                'location._id': mongoose.Schema.ObjectId(location._id)
              };
              bulk.find(query).upsert().updateOne({ $set: advertinstance });
            });

            console.log('Starting bulk execute');
            bulk.execute(function (err, result) {
              if (!err)
                console.log('Instance upsert executed for x_id ' + advert_copy._id + '. Inserted: ' + result.nInserted + ' Upserted: ' + result.nUpserted + ' Modified: ' + result.nModified);
              else
                console.log('Error with instance upsert. Msg: ' + err.message);
              callback(err, result);
            });
          }, function (err) {
            console.log('Completed all chunks');
            callback(err);
          });
        }
      };
      async.parallel(tasks, callback);
    }
  ], function (err, result) {
    console.log('Finishing function execution.');
    if (!err) {
      context.succeed(result);
    } else {
      context.fail(err);
    }
  });
}

var _convertTime = function (time, timezone) {
  var offsetMinutes = moment(time).tz(timezone).utcOffset();
  return moment(time).subtract(offsetMinutes, 'minutes').toDate();
};

