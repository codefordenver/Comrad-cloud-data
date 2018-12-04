const db = require('../../models');
const { Client }  = require('pg');
const mongoose = require('mongoose');

const importSource = "MusicBrainz";

mongoose.connect(process.env.MONGO_CONNECTION);
const pgClient = new Client()

module.exports = async function(event, context, callback) {   
  console.log('Starting MusicBrainz track import process.');
  try {
    //Connect to MusicBrainz DB and run a query for all albums
    //Import MusicBrainz albums into a Mongo database
    
    let importProgress = await db.ImportProgress.findOne();
    if (importProgress == null) {
      throw "No importProgress record found, run artist import first to create that record.";
    }
    let lastImportedArtistId = 0;
    let lastImportedTrackId = 0;
    if (importProgress['track_import'] != null) {
      if (importProgress['track_import']['last_imported_artist_id'] != null) {
        lastImportedArtistId = importProgress['track_import']['last_imported_artist_id'];
      }
      if (importProgress['track_import']['last_imported_track_id'] != null) {
        lastImportedReleaseGroupId = importProgress['track_import']['last_imported_track_id'];
      }
    } else {
      importProgress['track_import'] = { last_imported_artist_id: lastImportedArtistId, lastImportedTrackId: 0 };
    }
    
    // get artists from the database
    
    let numberOfReturnedRecords;
    let numberOfArtistsProcessed = 0;
    let numberOfTracksImported = 0;
    let trackLimit = 500;
    
    await pgClient.connect() //open postgresql connection to MusicBrainz database
    
    while (numberOfReturnedRecords == null || numberOfReturnedRecords > 0) {
      //TODO: how to filter this on -- either artist name = 'Various Artists', or the artist has listens?
      let artist = await db.Artist.findOne({"import_source.name": importSource, "import_source.id":{"$gt":lastImportedArtistId}}, 
        null,
        {
          skip: 0, 
          limit: 1,
          sort: "import_source.id"
      });
      
      numberOfReturnedRecords = artist != null ? 1 : 0;
      
      // query the MusicBrainz database for albums by that artist
      
      console.log('Looking for tracks for the artist: ' + artist['name'] + ' (source id: ' + artist['import_source']['id'] + ')');
      
      let numberOfTrackRecords;

      //loop through records in batches: this helps with artists that have an enormous number of albums, like "Various Artists"
      while (typeof numberOfTrackRecords == 'undefined' || numberOfTrackRecords > 0) { 
        if (lastImportedTrackId > 0) {
          console.log("lastImportedTrackId: " + lastImportedTrackId);
        }
        //TODO: we need the album name in this as album_name
        let res = await pgClient.query(
          'SELECT track.length / 1000 AS duration_in_seconds, ' + 
            'track.position AS track_number,  ' + 
            'track.id AS track_id, ' +
            'medium.position AS disk_number, ' + 
            'track.id, track.name, ' + 
            'artist_credit.name AS artists, ' + 
            'artist_count ' + 
          'FROM track  ' + 
          'INNER JOIN medium ON medium.id = track.medium ' + 
          'INNER JOIN artist_credit ON artist_credit.id = track.artist_credit ' + 
          'WHERE artists LIKE \'%$1%\' ' + 
          'AND release_group.id > $2 ' +
          'ORDER BY track.id' +
          'LIMIT $3', 
          [artist['name'], lastImportedTrackId, trackLimit]
        );
        
        let newTracks = [];
        let idsToImport = [];
        
        numberOfTrackRecords = res.rows.length;
        
        //TODO: ensure this is correct - both w/query and with calculation (if exploding on commas, may need to check something like Medeski Martin & Wood
        let artistNames = row['artists'].split(',');
        let artistIds = [];
        for (let a = 0; a < artistNames.length; a++) {
          let artistRecord = await db.Artists.findOne({"name": artists[a]});
          if (artistRecord == null) {
            console.error('artist not found: ' + artists[a]);
          }
          artistIds.push(artistRecord['id']); //TODO: is this right, or do I need to do something liek objectId? ...or, just do the record itself?
        }
        
        let album = await db.Albums.findOne({"name":row['album_name']});
        if (typeof album == 'undefined') console.error('could not find album: ' + row['album_name']);
        
        await Promise.all(res.rows.map(async row => {
          lastImportedTrackId = Math.max(lastImportedTrackId, row['track_id']);
          idsToImport.push(row['track_id']);
          let newTrack = new db.Track({
            name: row['name'],
            album: album.id, 
            duration_in_seconds: row['duration_in_seconds'],
            track_number: row['track_number'],
            disk_number: row['disk_number'],
            artists: artistIds,
            import_source: {
              'name': importSource,
              'id': row['track_id']
            }
          });
          newTracks.push(newTrack);
        }));
        
        let existingTracks = await db.Track.find({"import_source.name":importSource,"import_source.id":{"$in":idsToImport}});
        let filteredTracks = newTracks.filter(function(newTrack) {
        for (let i = 0; i < existingTracks.length; i++) {
            if (existingTracks[i]['import_source']['id'] == newTrack['import_source']['id']) {
              return false;
            }
          }
          return true;
        });
        
        if (filteredTracks.length > 0) {
          await db.Track.collection.insertMany(filteredTracks);
          numberOfTracksImported += filteredTracks.length;
        }
        
        importProgress['track_import']['last_imported_track_id'] = lastImportedTrackId;
        await importProgress.save();
      }
      
      lastImportedTrackId = Math.max(lastImportedTrackId, track['import_source']['id']);
      
      lastImportedTrackId = 0;
      importProgress['track_import']['last_imported_track_id'] = lastImportedTrackId;
      importProgress['track_import']['last_imported_artist_id'] = lastImportedArtistId;
      importProgress['track_import']['last_processed_date'] = Date.now();
      await importProgress.save();
      
      numberOfArtistsProcessed++;
      
    }
        
    await pgClient.end(); //close the postgresql connection
    
    console.log('Finished MusicBrainz track import process: ' + numberOfTracksImported + ' tracks from ' + numberOfArtistsProcessed + ' artists were imported');
    
    callback(null, 'Track import success: ' + numberOfAlbumsImported + ' tracks from ' + numberOfArtistsProcessed + ' artists were imported');
    
  } catch (error) {
    console.error('ERROR: MusicBrainz track import process');
    console.error(error.message);
    callback("Error importing MusicBrains tracks: " + error.message); 
  } 
}