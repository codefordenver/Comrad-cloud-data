// This script should run after importing tracks

const db = require('../../models');
const mongoose = require('mongoose');

const importSource = 'ListenBrainz';

mongoose.connect(process.env.MONGO_CONNECTION);

module.exports = async function(event, context, callback) {   
  console.log('Starting ListenBrainz track import process.');
  try {
    
    let allTrackListens = {};
    let trackCount = 0;
    
    let lineReader = require('readline').createInterface({
      input: require('fs').createReadStream(process.env.LISTENBRAINZ_DATABASE_FILE_PATH)
    });
    
    //loop through all lines in the file
    await new Promise((resolve, reject) => {
      lineReader.on('line', function (line) {
        line = line.substring(line.indexOf("{")); //trim to where the JSON starts
        //split the line since each line contains three JSON sets of statistics
        //      the first is artists, the second releasess, the third tracks
        var lineParts = line.split(/\}\t\{/g);
        let trackListens;
        //trim the track list at the last }, because everything after it is a timestamp
        lineParts[2] = lineParts[2].substring(0, lineParts[2].lastIndexOf("}") + 1);
        trackListens = JSON.parse("{" + lineParts[2].replace(/\\\\/g, "\\")); // backslashes are double-escaped in the listenbrainz dump
        trackListens['all_time'].forEach(function(tl) {          
          if (typeof allTrackListens[tl.artist_name] == 'undefined') {
            allTrackListens[tl.artist_name] = {};
          }
          if (typeof allTrackListens[tl.artist_name][tl.track_name] == 'undefined') {
            allTrackListens[tl.artist_name][tl.track_name] = parseInt(tl.listen_count);
            trackCount++;
          } else {
            allTrackListens[tl.artist_name][tl.track_name] += parseInt(tl.listen_count);
          }
        });
      }).on('close', function () {
        resolve(true);
      });
    });
    
    console.log("Finished parsing ListenBrainz data. Found " + Object.keys(allTrackListens).length + " artists with " + trackCount + " tracks");
    
    let importProgress = await db.ImportProgress.findOne();
    let startIndex = 0;
    
    if (importProgress['track_listen_import'] != null && importProgress['track_listen_import']['last_imported_index'] != null) {
      startIndex = importProgress['track_listen_import']['last_imported_index'];
    } else {
      importProgress['track_listen_import'] = {'last_imported_index':startIndex};
    }
    
    console.log("Starting with index: " + startIndex);
    
    //save listens to database, looping through them in batches
    let listenKeys = Object.keys(allTrackListens);
    let batchSize = 600;
    for (var i = startIndex; i < listenKeys.length; i = i + batchSize) {
      //save import progress
      importProgress['track_listen_import']['last_imported_index'] = i;
      importProgress['track_listen_import']['last_imported_date'] = Date.now();
      importProgress.save();
      
      //loop through artists
      let listenKeysGroup = listenKeys.slice(i, Math.min(i + batchSize, listenKeys.length));
      let artists = await db.Artist.find({"name": { "$in": listenKeysGroup }, "listens.listens": {"$gte": 1}}); //only find artists that have listens already from the artist listen import
      
      let bulkOperations = [];
      
      //loop through artists and find each track in the database
      for (var j = 0; j < artists.length; j++) {
        let artist = artists[j];
        await Promise.all(Object.keys(allTrackListens[artist.name]).map(async track_name => {
          //console.log('track name: ' + track_name + '||| artist id: ' + artist.id + '||| artist name: ' + artist.name + '||| listens:' + allTrackListens[artist.name][track_name]);
          
          //bulk operation for when a ListenBrainz field does not exist
          bulkOperations.push({
            "updateOne": {
              "filter": {
                "name":track_name,
                "artists": { "$in": [artist.id] },
                "listens.import_source":{"$nin":[importSource]}
              },
              "update": {
                "$push": {
                  "listens": {
                    'import_source': importSource,
                    'listens': allTrackListens[artist.name][track_name]
                  }
                }
              }
            }
          });
          
          // TODO: in a future version, need a bulk operation for records that dDO exist. perhaps delete the record, then re-add it? 
          
        }));
      }
      if (bulkOperations.length > 0) {
        await db.Track.bulkWrite(bulkOperations);
      }
      console.log('processed listens for ' + bulkOperations.length + ' tracks');
    }
    
    console.log('Finished ListenBrainz track listen import');
    
    callback(null, "ListenBrainz track listen import success");
    
  } catch (error) {
    console.error('ERROR: ListenBrainz artist listen import process');
    console.error(error.message);
    callback("Error importing ListenBrainz artist listens: " + error.message); 
  } 
}