const db = require('../../models');
const { Client }  = require('pg');
const mongoose = require('mongoose');
const fs = require('fs');

mongoose.connect(process.env.MONGO_CONNECTION);

module.exports = async function(event, context, callback) {   
  console.log('Starting data dump from Mongo database...');
  try {
    
    console.log("Starting dump of artists file...");
    
    let artistsFile = process.env.DATABASE_DUMP_FOLDER + 'artists.data';
    if (fs.existsSync(artistsFile)) {
      fs.unlinkSync(artistsFile);
    }
    
    var query = await db.Artist.find({"listens.listens":{"$gte":1}});
    let artistIds = [];
    query.forEach(function (doc) {
      //remove data points that don't need to be imported to the main Comrad music library database
      let docClone = JSON.parse(JSON.stringify(doc)); //clone the document so deleting keys works
      delete docClone.__v;
      delete docClone.import_source;
      delete docClone.created_at;
      delete docClone.updated_at;
      delete docClone.listens;
      fs.appendFileSync(artistsFile, JSON.stringify(docClone) + "\n");
      artistIds.push(doc._id);
    });
    
    console.log("Finished dump of artists file");
    
    console.log("Starting dump of tracks file...");
    
    let tracksFile = process.env.DATABASE_DUMP_FOLDER + 'tracks.data';
    if (fs.existsSync(tracksFile)) {
      fs.unlinkSync(tracksFile);
    }
    
    query = await db.Track.find({"listens.listens":{"$gte":1}});
    let albumIdsForTracks = [];
    query.forEach(function (doc) {
      //remove data points that don't need to be imported to the main Comrad music library database
      let docClone = JSON.parse(JSON.stringify(doc)); //clone the document so deleting keys works
      delete docClone.__v;
      delete docClone.import_source;
      delete docClone.created_at;
      delete docClone.updated_at;
      delete docClone.tickets
      fs.appendFileSync(tracksFile, JSON.stringify(docClone) + "\n");
      if (albumIdsForTracks.indexOf(doc.album) == -1) {
        albumIdsForTracks.push(doc.album);
      }
    });
    
    console.log("Finished dump of tracks file");
    
    console.log("Starting dump of albums file...");
    
    let albumsFile = process.env.DATABASE_DUMP_FOLDER + 'albums.data';
    if (fs.existsSync(albumsFile)) {
      fs.unlinkSync(albumsFile);
    }
    
    query = await db.Album.find({"_id":{"$in":albumIdsForTracks}}); //get albums with tracks in the data dump
    query.forEach(function (doc) {
      //remove data points that don't need to be imported to the main Comrad music library database
      let docClone = JSON.parse(JSON.stringify(doc)); //clone the document so deleting keys works
      delete docClone.__v;
      delete docClone.import_source;
      delete docClone.created_at;
      delete docClone.updated_at;
      fs.appendFileSync(albumsFile, JSON.stringify(docClone) + "\n");
    });
    
    console.log("Finished dump of artists file");
    
    callback(null, "Data dump successful.");
    
  } catch (error) {
    console.error('ERROR: while generating data dump');
    console.error(error.message);
    callback("Error generating data dump: " + error.message); 
  } 
}