// This script should run after importing artists,
// and before importing tracks/albums

const db = require('../../models');
const mongoose = require('mongoose');

const importSource = 'ListenBrainz';

mongoose.connect(process.env.MONGO_CONNECTION);

module.exports = async function(event, context, callback) {   
  console.log('Starting ListenBrainz artist listen import process.');
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
    
    console.log("Finished parsing ListenBrainz data.");
    
    //save listens to database, looping through them in batches
    let listenKeys = Object.keys(allArtistListens);
    for (var i = 0; i < listenKeys.length; i = i + 1000) {
      let listenKeysGroup = listenKeys.slice(i, Math.min(i + 1000, listenKeys.length));
      let artists = await db.Artist.find({"name": { "$in": listenKeysGroup }});
      let updatedArtists = [];
      let bulkOperations = [];
      
      artists.forEach(function(artist) {
        
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
          artistUpdate["listens." + listenBrainzIndex + ".listens"] = allArtistListens[artist.name];
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
    
    console.log('Finished ListenBrainz artist listen import');
    
    callback(null, "ListenBrainz artist import success");
    
  } catch (error) {
    console.error('ERROR: ListenBrainz artist listen import process');
    console.error(error.message);
    callback("Error importing ListenBrainz artist listens: " + error.message); 
  } 
}