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
const AdvertInstanceModel = mongoose.model('advertinstances', no_schema, 'advertinstances');
const AdvertModel = mongoose.model('adverts', no_schema, 'adverts');
const LocationModel = mongoose.model('locations', no_schema, 'locations');

module.exports.run = (event, context, callback) => {
  event = JSON.parse(event.body);
  mongodb_config.database_name = event.database_name;

  console.log('Input: ' + JSON.stringify(event));

  const advert_id = mongoose.Types.ObjectId(event.advert_id);
  const location_ids = _.map(event.location_ids, function (location_id) {  // Copy the ids for the parallel tasks
    return mongoose.Types.ObjectId(location_id);
  });

  async.waterfall([
    function (callback) {
      //connect to database if not already
      database.connect(mongodb_config, callback);
    },
    function (callback) {
      //get advert and locations
      var get_tasks = {
        advert: function (callback) {
          AdvertModel.findOne({ _id: advert_id }).lean().exec(function (err, result) {
            console.log('Advert find ' + advert_id + '. Title: ' + result.title);
            callback(err, result);
          });
        },
        locations: function (callback) {
          LocationModel.find({
            '_id': {
              $in: location_ids
            }
          }).lean().exec(callback);
        }
      };
      async.parallel(get_tasks, function (err, result) {
        if (!err) {
          event.advert = result.advert;
          event.locations = result.locations;
        }
        callback(err);
      });
    },
    function (callback) {
      var tasks = {
        markAsDelete: function (callback) {
          AdvertInstanceModel.update(
            {
              'advert.id': advert_id,
              'location._id': { $nin: location_ids }
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
                console.log('Advertinstance updated with advert id ' + event.advert_id + '. Found ' + result.n + ' and marked ' + result.nModified + ' as deleted');
              else
                console.log('Error with Advertinstance update. Msg: ' + err.message);
              callback(err, result);
            }
          );
        },
        upsert: function (callback) {
          if (_.isEmpty(location_ids)) {
            return callback(null);
          }

          var chunks = _.chunk(location_ids, 100);
          async.eachSeries(chunks, function (chunk, callback) {
            var advert_copy = _.cloneDeep(event.advert);
            var bulk = AdvertInstanceModel.collection.initializeUnorderedBulkOp();

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
                'advert._id': mongoose.Types.ObjectId(advert_copy._id),
                'location._id': mongoose.Types.ObjectId(location._id)
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
      var res = {
        'isBase64Encoded': false,
        'statusCode': 200,
        'headers': {},
        'body': JSON.stringify(result)
      };
      context.succeed(res);
    } else {
      var err = {
        'isBase64Encoded': false,
        'statusCode': 502,
        'headers': {},
        'body': JSON.stringify(res)
      };
      context.fail(err);
    }
  });
}

var _convertTime = function (time, timezone) {
  var offsetMinutes = moment(time).tz(timezone).utcOffset();
  return moment(time).subtract(offsetMinutes, 'minutes').toDate();
};

