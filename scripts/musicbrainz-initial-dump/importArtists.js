//NEXT STEPS:
//2. be sure duplicates are not saved
//5. test in lambda
//6. execute locally

const db = require('../../models');
const { Client }  = require('pg');
const mongoose = require('mongoose');

const importSource = "MusicBrainz";

mongoose.connect(process.env.MONGO_CONNECTION);
const pgClient = new Client()

exports.myHandler = async function(event, context, callback) {   
  console.log('Starting MusicBrainz import process.');
  try {
    //Connect to MusicBrainz DB and run a query for all artists
    //Import MusicBrainz artists into a Mongo database
    
    var importProgress = await db.ImportProgress.findOne();
    if (importProgress == null) {
      importProgress = new db.ImportProgress({artist_import: { last_imported_id: 0 }});
    }
    
    var recordsToProcess = 50;
    var numberOfReturnedRecords;
    
    await pgClient.connect() //open postgresql connection to MusicBrainz database
    
    while (numberOfReturnedRecords == null || numberOfReturnedRecords > 0) { // loop until there are no musicbrainz records being returned by the below query
    
      console.log("Querying MusicBrainz for artist data, id > " + importProgress['artist_import']['last_imported_id'] + ", LIMIT " + recordsToProcess);
    
      let res = await pgClient.query(
        'SELECT id, name FROM musicbrainz.artist WHERE id > $1 ORDER BY id LIMIT $2', 
        [importProgress['artist_import']['last_imported_id'], recordsToProcess]
      );
      
      numberOfReturnedRecords = res.rows.length;
      
      await Promise.all(res.rows.map(async row => {
        // be sure this record is not already in Mongo
        let existingArtist = await db.Artist.findOne({"import_source":{"name":importSource,"id":row['id']}});
        if (existingArtist != null) return;
        //save the artist to mongo
        let newArtist = new db.Artist({
          name: row['name'],
          import_source: {
            'name': importSource,
            'id': row['id']
          }
        });
        await newArtist.save();
      }));
      
      importProgress['artist_import']['last_processed_date'] = Date.now();
      importProgress['artist_import']['last_imported_id'] += recordsToProcess;
      importProgress.save();
      
    }
    
    await pgClient.end(); //close the postgresql connection
    
    console.log('Finished MusicBrainz import process.');
    
  } catch (error) {
    //todo: better way to error handle async/await?
    console.error('ERROR: MusicBrainz import process');
    throw error;
  }
  
  //needed for lambda:
  //callback(null, "some success message");
  // or 
  // callback("some error type"); 
}