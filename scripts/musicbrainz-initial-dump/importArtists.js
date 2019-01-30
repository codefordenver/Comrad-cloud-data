const db = require('../../models');
const { Client }  = require('pg');
const mongoose = require('mongoose');

const importSource = "MusicBrainz";

mongoose.connect(process.env.MONGO_CONNECTION);
const pgClient = new Client()

module.exports = async function(event, context, callback) {   
  console.log('Starting MusicBrainz artist import process.');
  try {
    //Connect to MusicBrainz DB and run a query for all artists
    //Import MusicBrainz artists into a Mongo database
    
    let importProgress = await db.ImportProgress.findOne();
    if (importProgress == null) {
      importProgress = new db.ImportProgress({artist_import: { last_imported_id: 0 }});
    }
    
    let recordsToProcess = 1500;
    let numberOfReturnedRecords;
    let numberOfArtistsImported = 0;
    
    await pgClient.connect() //open postgresql connection to MusicBrainz database
    
    while (numberOfReturnedRecords == null || numberOfReturnedRecords > 0) { // loop until there are no musicbrainz records being returned by the below query
    
      console.log("Querying MusicBrainz for artist data, id > " + importProgress['artist_import']['last_imported_id'] + ", LIMIT " + recordsToProcess);
    
      let res = await pgClient.query(
        'SELECT id, name FROM musicbrainz.artist WHERE id > $1 ORDER BY id LIMIT $2', 
        [importProgress['artist_import']['last_imported_id'], recordsToProcess]
      );
      
      numberOfReturnedRecords = res.rows.length;
      
      let newArtists = [];
      let idsToImport = [];
      
      await Promise.all(res.rows.map(async row => {
        idsToImport.push(row['id']);
        let newArtist = new db.Artist({
          name: row['name'],
          import_source: {
            'name': importSource,
            'id': row['id']
          }
        });
        newArtists.push(newArtist);
      }));
      
      // be sure the records we are inserting are not already in Mongo
      let existingArtists = await db.Artist.find({"import_source.name":importSource,"import_source.id":{"$in":idsToImport}});
      var filteredArtists = newArtists.filter(function(newArtist) {
        for (let i = 0; i < existingArtists.length; i++) {
          if (existingArtists[i]['import_source']['id'] == newArtist['import_source']['id']) {
            return false;
          }
        }
        return true;
      });
      
      if (filteredArtists.length > 0) {
        await db.Artist.collection.insertMany(filteredArtists);
      }
      
      let maxId = 0;
      if (idsToImport.length > 0) {
        maxId = idsToImport.reduce(function(a, b) {
            return Math.max(a, b);
        });
      }
      
      importProgress['artist_import']['last_imported_date'] = Date.now();
      importProgress['artist_import']['last_imported_id'] = Math.max(maxId, importProgress['artist_import']['last_imported_id']);
      importProgress.save();
      numberOfArtistsImported++;
      
    }
    
    await pgClient.end(); //close the postgresql connection
    
    console.log('Finished MusicBrainz artist import process: ' + numberOfArtistsImported + ' artists were imported');
    
    callback(null, "Artist import success: " + numberOfArtistsImported + ' artists were imported');
    
  } catch (error) {
    console.error('ERROR: MusicBrainz artist import process');
    console.error(error.message);
    callback("Error importing MusicBrains artists: " + error.message); 
  } 
}