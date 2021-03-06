// This script should run after importing artists,
// and before importing tracks/albums

const db = require('../../models');
const mongoose = require('mongoose');

const importSource = 'ListenBrainz';

mongoose.connect(process.env.MONGO_CONNECTION);

module.exports = async function(event, context, callback) {   
  console.log('Starting ListenBrainz artist listen import process. (start time: ' + Date.now() + ')');
  try {
    
    var allArtistListens = {};
    
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
        let artistListens;
        artistListens = JSON.parse(lineParts[0].replace(/\\\\/g, "\\") + "}"); // backslashes are double-escaped in the listenbrainz dump
        artistListens['all_time'].forEach(function(al) {          
          if (typeof allArtistListens[al.artist_name] == 'undefined') {
            allArtistListens[al.artist_name] = parseInt(al.listen_count);
          } else {
            allArtistListens[al.artist_name] += parseInt(al.listen_count);
          }
        });
      }).on('close', function () {
        resolve(true);
      });
    });
    
    console.log("Finished parsing ListenBrainz data. Found " + Object.keys(allArtistListens).length + " artists");
    
    let importProgress = await db.ImportProgress.findOne();
    let startIndex = 0;
    
    if (importProgress['artist_listen_import'] != null && importProgress['artist_listen_import']['last_imported_index'] != null) {
      startIndex = importProgress['artist_listen_import']['last_imported_index'];
    } else {
      importProgress['artist_listen_import'] = {'last_imported_index':startIndex};
    }
    
    console.log("Starting with index: " + startIndex);
    
    //save listens to database, looping through them in batches
    let listenKeys = Object.keys(allArtistListens);
    for (var i = startIndex; i < listenKeys.length; i = i + 1000) {
      //save import progress
      importProgress['artist_listen_import']['last_imported_index'] = i;
      importProgress['artist_listen_import']['last_imported_date'] = Date.now();
      importProgress.save();
      
      let listenKeysGroup = listenKeys.slice(i, Math.min(i + 1000, listenKeys.length));
      let artists = await db.Artist.find({"name": { "$in": listenKeysGroup }});
      //only update one record for each artist name -- the MusicBrainz import results in duplicate artist records
      let uniqueArtists = [];
      let uniqueArtistNames = [];
      artists.forEach(function(artist) {
        if (uniqueArtistNames.indexOf(artist.name) == -1) {
          uniqueArtistNames.push(artist.name);
          uniqueArtists.push(artist);
        }
      });
      console.log('Got ' + artists.length + ' artist records (' + uniqueArtists.length + ' unique names) from Mongo for batch');
      let updatedArtists = [];
      let bulkOperations = [];
      
      uniqueArtists.forEach(function(artist) {
        
        updatedArtists.push(artist.name);
        
        let listenBrainzIndex = artist.listens.findIndex(function(r) {
          return r.import_source == importSource;
        });
        
        let artistUpdate = {
          "updated_at": Date.now()
        };

        if (listenBrainzIndex == -1) {
          artistUpdate["$push"] = {
              "listens": {
                'import_source': importSource,
                'listens': allArtistListens[artist.name]
              }
          };
        } else {
          console.log(artist.name + " already has listens record for ListenBrainz, replacing record");
          let indexKey = "listens." + listenBrainzIndex + ".listens";
          artistUpdate["$set"] = { indexKey: allArtistListens[artist.name] };
        }
        bulkOperations.push({
          "updateOne": {
            "filter": {"name":artist.name},
            "update": artistUpdate
          }
        });
      });
      
      
      
      await db.Artist.bulkWrite(bulkOperations);
      console.log("saved artists: " + updatedArtists.join(" || "));
    }
    
    //save import progress
    importProgress['artist_listen_import']['last_imported_index'] = Math.min(i, listenKeys.length);
    importProgress['artist_listen_import']['last_imported_date'] = Date.now();
    importProgress.save();
    
    console.log('Finished ListenBrainz artist listen import  (end time: ' + Date.now() + ')');
    
    callback(null, "ListenBrainz artist import success");
    
  } catch (error) {
    console.error('ERROR: ListenBrainz artist listen import process');
    console.error(error.message);
    callback("Error importing ListenBrainz artist listens: " + error.message); 
  } 
}