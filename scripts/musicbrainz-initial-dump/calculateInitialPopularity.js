const db = require('../../models');
const { Client }  = require('pg');
const mongoose = require('mongoose');

mongoose.connect(process.env.MONGO_CONNECTION);

module.exports = async function(event, context, callback) {   
  console.log('Starting data dump from Mongo database...');
  try {
    
    console.log("Starting to calculate artist/album popularity...");
    
    let maxListenArtist = await db.Artist.findOne({}, null, {"sort":{"listens.listens":-1}});
    let listens = 0;
    maxListenArtist.listens.forEach(function(l) {
      listens += l.listens;
    });
    console.log("Max listens are " + listens + " by " + maxListenArtist['name']);
    let maxArtistListensLog = Math.log(listens); //use logarithmic scale since differences in listens can be huge
    
    let artists = await db.Artist.find({"listens.listens":{"$gte":1}});
    
    let bulkOperations = [];
    let bulkAlbumOperations = [];
    artists.forEach(function (artist) {
      let listens = 0;
      artist.listens.forEach(function(l) {
        listens += l.listens;
      });
      let popularity = Math.max(1, Math.round(Math.log(listens) / maxArtistListensLog * 100));
      bulkOperations.push({
        "updateOne": {
          "filter": {"_id":artist._id},
          "update": {"popularity":popularity}
        }
      });
      
      //
      // album popularity
      // currently, we're just taking the popularity from the artist
      //
      bulkAlbumOperations.push({
        "updateMany": {
          "filter": { "artist":artist._id},
          "update": {"popularity":popularity}
        }
      });
      
    });
    console.log('Running ' + bulkOperations.length + ' artist operations on database');
    await db.Artist.bulkWrite(bulkOperations);
    console.log('Running ' + bulkAlbumOperations.length + ' album operations on database');
    await db.Album.bulkWrite(bulkAlbumOperations);
    
    console.log("Finished calculating artist/album popularity");
    
    //
    // track popularity
    //
    
    console.log("Starting to calculate track popularity...");
    
    let maxListenTrack = await db.Track.findOne({}, null, {"sort":{"listens.listens":-1}});
    listens = 0;
    maxListenTrack.listens.forEach(function(l) {
      listens += l.listens;
    });
    console.log("Max listens are " + listens + " by " + maxListenTrack['name']);
    let maxTrackListensLog = Math.log(listens); //use logarithmic scale since differences in listens can be huge
    
    let tracks = await db.Track.find({"listens.listens":{"$gte":1}});
    
    bulkOperations = [];
    tracks.forEach(function (track) {
      let listens = 0;
      track.listens.forEach(function(l) {
        listens += l.listens;
      });
      let popularity = Math.max(1, Math.round(Math.log(listens) / maxTrackListensLog * 100));
      bulkOperations.push({
        "updateOne": {
          "filter": {"_id":track._id},
          "update": {"popularity":popularity}
        }
      });
    });
    console.log('Running ' + bulkOperations.length + ' operations on database');
    await db.Track.bulkWrite(bulkOperations);
    
    console.log("Finished calculating artist popularity");
    
    callback(null, "Popularity calculation successful.");
    
  } catch (error) {
    console.error('ERROR: while generating data dump');
    console.error(error.message);
    callback("Error generating data dump: " + error.message); 
  } 
}