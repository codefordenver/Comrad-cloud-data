// This script should run after importing tracks

const db = require('../../models');
const mongoose = require('mongoose');

const importSource = 'ListenBrainz';

mongoose.connect(process.env.MONGO_CONNECTION);

module.exports = async function(event, context, callback) {   
  console.log('Starting ListenBrainz track import process.');
  try {
    
    //TODO: open file and read each line
    
    let allTrackListens = {};
    
    //start loop through lines here
    
    for (let i = 0; i < lines.length; i++) {
      line = lines[i];
    
    //TODO: convert JSON in line to an object --- just need to run a regex to get the first + last {} (greedy identifier)
    //      actually - there are three objects in a line. can look for } { -- without a period and split these
    //      the first is artists, the second releasess, the third tracks
      let trackListens;
      
      trackListens.forEach(function(tl) {
        if (typeof allTrackListens[tl.artist_name] == 'undefined') {
          allTrackListens[tl.artist_name] = {};
          allTrackListens[tl.artist_name][tl.track_name] = listen_count.listen_count;
        } else {
          if (typeof allTrackListens[tl.artist_name][tl.track_name] == 'undefined') {
            allTrackListens[tl.artist_name][tl.track_name] = tl.listen_count;
          } else {
            allTrackListens[tl.artist_name][tl.track_name] += tl.listen_count;
          }
        }
      });
    }    
    
    //end loop through lines here
    
    //TODO: ensure that a map function will map the keys this way - I may need an alternate way to make these promises
    await Promise.all(allTrackListens.map(async artist_name => { //,tracks
      let artist = await db.Artist.findOne({"name": artist_name});
      if (typeof artist == 'undefined') return;
      await Promise.all(tracks.map(async track_name => { //, listens
        let track = await db.Track.findOne({"name": track_name, artists: { $contains: artist.id }});
        if (typeof track == 'undefined') return;
        if (typeof track['listens'] == 'undefined') track['listens'] = [];
        let listenBrainzRecord = track['listens'].filter(function(r) {
          return r.import_source == importSource;
        });
        if (listenBrainzRecord.length == 0) {
          track['listens'].push({
            'import_source': importSource,
            'listens': listens
          });
        } else {
          listenBrainzRecord[0]['listens'] = listens;
        }
        await track.save();
      }));
    }));
    
    console.log('Finished ListenBrainz track listen import');
    
    callback(null, "ListenBrainz track listen import success");
    
  } catch (error) {
    console.error('ERROR: ListenBrainz artist listen import process');
    console.error(error.message);
    callback("Error importing ListenBrainz artist listens: " + error.message); 
  } 
}