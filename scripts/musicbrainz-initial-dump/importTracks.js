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
    if (importProgress['track_import'] != null) {
      if (importProgress['track_import']['last_imported_artist_id'] != null) {
        lastImportedArtistId = importProgress['track_import']['last_imported_artist_id'];
      }
      if (importProgress['track_import']['last_imported_track_id'] != null) {
        lastImportedReleaseGroupId = importProgress['track_import']['last_imported_track_id'];
      }
    } else {
      importProgress['track_import'] = { last_imported_artist_id: lastImportedArtistId };
    }
    
    // get artists from the database
    
    let numberOfReturnedRecords;
    let numberOfArtistsProcessed = 0;
    let numberOfTracksImported = 0;
    let trackLimit = 500;
    
    await pgClient.connect() //open postgresql connection to MusicBrainz database
    
    while (numberOfReturnedRecords == null || numberOfReturnedRecords > 0) {
      let artists = await db.Artist.find({
          "import_source.name": importSource, 
          "import_source.id": {"$gt":lastImportedArtistId},
          "listens.listens": {"$gt":10}
        }, 
        null,
        {
          skip: 0, 
          limit: lastImportedArtistId > 100000 ? 100 : (lastImportedArtistId > 2000 ? 15 : 5), // for artists occuring first in the database, look at fewer at a time, since these tend to be the more popular ones with tons of tracks
          sort: "import_source.id"
      });
      
      numberOfReturnedRecords = artists.length;
      artists.forEach(function(artist) {
        lastImportedArtistId = Math.max(lastImportedArtistId, artist['import_source']['id']);
      });
      
      if (numberOfReturnedRecords == 0) {
        break;
      }
      
      // query the MusicBrainz database for tracks by that artist
      
      console.log('Looking for tracks for the artists: ' + artists.map(function(artist) { return artist['name'] + " (source id: " + artist['import_source']['id'] + ')'; }).join(", "));

      let parameters = artists.map(function(artist) { return artist['import_source']['id'] });
      let res = await pgClient.query(
        'SELECT track.length / 1000 AS duration_in_seconds, ' + 
          'track.position AS track_number,  ' + 
          'track.id AS track_id, ' +
          'medium.position AS disk_number, ' + 
          'track.id, track.name, ' + 
          'artist_credit.id AS artist_credit_id, ' + 
          'artist_count, ' + 
          'release.id AS release_id ' +
        'FROM track  ' + 
        'INNER JOIN medium ON medium.id = track.medium ' + 
        'INNER JOIN release ON medium.release = release.id ' +
        'INNER JOIN artist_credit ON artist_credit.id = track.artist_credit ' + 
        'WHERE artist_credit.id IN (SELECT artist_credit_name.artist_credit FROM artist_credit_name WHERE artist IN (' +
            artists.map(function(artist, idx) { return '$' + (idx + 1); }).join(',') + 
        ')) ',
        parameters
      );
      console.log('Finished tracks query');
      
      let newTracks = [];
      let idsToImport = [];
      
      //Get the artist IDs and album IDs for each track
      console.log('looking for the tracks\' artists and album records in Mongo');
      let artistCreditData = {};
      let releaseIdsUsedByTracks = [];
      await Promise.all(res.rows.map(async row => {
        artistCreditData[row['artist_credit_id']] = [];
        releaseIdsUsedByTracks.push(row['release_id']);
      }));
      let artistCreditResult = await pgClient.query(
        'SELECT artist as artist_id, ' + 
          'artist_credit as artist_credit_id  ' + 
        'FROM artist_credit_name ' + 
        'WHERE artist_credit IN (' +
            Object.keys(artistCreditData)
              .map(function(artist, idx) { return '$' + (idx + 1); })
              .join(',') + //.slice is used to remove trailing comma
        ') ' +
        'ORDER BY artist_credit',
        Object.keys(artistCreditData)
      );
      let artistIdsInCredits = [];
      await Promise.all(artistCreditResult.rows.map(async row => {
        artistCreditData[row['artist_credit_id']].push(row['artist_id']);
        artistIdsInCredits.push(row['artist_id']);
      }));
      let artistsInCredits = await db.Artist.find({"import_source.name":importSource,"import_source.id": { "$in": artistIdsInCredits }});
      let artistInCreditIds = {};
      artistsInCredits.forEach(function(artist) {
        artistInCreditIds[artist['import_source']['id']] = artist['_id'];
      });
      
      
      let albumsUsedByTracks = await db.Album.find({"import_source.name":importSource,"import_source.id": { "$in": releaseIdsUsedByTracks }});
      let albumIdsUsedByTracksIds = {};
      albumsUsedByTracks.forEach(function(album) {
        albumIdsUsedByTracksIds[String(album['import_source']['id'])] = album['_id'];
      });
      
      console.log('finished finding the tracks\' artists and album records in Mongo');
      
      console.log('preparing data for insert into mongo');
      
      await Promise.all(res.rows.map(async row => { 
        
        row['artist_ids'] = [];
        artistCreditData[row['artist_credit_id']].forEach(function(artistId) {
          row['artist_ids'].push(artistInCreditIds[artistId]);
        });
        
        if (row['artist_ids'].length == 0) { 
          // if no artists were found, don't add this track
          // this can happen on many compilations
          return;
        }
        
        
        if (Object.keys(albumIdsUsedByTracksIds).indexOf(String(row['release_id'])) == -1) { 
          // many release_ids will not exist, because they were filtered by the importAlbums.js script to only include the first release. we'll skip these
          // TODO: a future query improvement could be to change the initial PostgreSQL query for tracks so it excludes any release_ids 
          // that aren't in Mongo. I'm not sure this is feasible though because there are so many release_ids and I'm not sure the best 
          // way to get them into that query.
          return;
        }
        let album = albumIdsUsedByTracksIds[row['release_id']];
        
        if (row['name'].length < 1000) { //don't import tracks with names longer than 1000 characters, it can cause indexing issues
          idsToImport.push(row['track_id']);
          let newTrack = new db.Track({
            name: row['name'],
            album: album['_id'], 
            duration_in_seconds: row['duration_in_seconds'],
            track_number: row['track_number'],
            disk_number: row['disk_number'],
            artists: row['artist_ids'],
            import_source: {
              'name': importSource,
              'id': row['track_id']
            }
          });
          newTracks.push(newTrack);
        }
      }));
      
      console.log('finished step 1/2 preparing data for insert into mongo');
      
      let existingTracks = await db.Track.find({"import_source.name":importSource,"import_source.id":{"$in":idsToImport}});
      let filteredTracks = newTracks.filter(function(newTrack) {
      for (let i = 0; i < existingTracks.length; i++) {
          if (existingTracks[i]['import_source']['id'] == newTrack['import_source']['id']) {
            return false;
          }
        }
        return true;
      });
      
      console.log('finished step 2/2 preparing data for insert into mongo');
      
      console.log('inserting data into mongo');
      
      if (filteredTracks.length > 0) {
        await db.Track.collection.insertMany(filteredTracks);
        numberOfTracksImported += filteredTracks.length;
      }
      
      console.log('finished inserting data into mongo');
      
      importProgress['track_import']['last_imported_artist_id'] = lastImportedArtistId;
      importProgress['track_import']['last_imported_date'] = Date.now();
      await importProgress.save();
      
      numberOfArtistsProcessed++;
      
    }
        
    await pgClient.end(); //close the postgresql connection
    
    console.log('Finished MusicBrainz track import process: ' + numberOfTracksImported + ' tracks from ' + numberOfArtistsProcessed + ' artists were imported');
    
    callback(null, 'Track import success: ' + numberOfTracksImported + ' tracks from ' + numberOfArtistsProcessed + ' artists were imported');
    
  } catch (error) {
    console.error('ERROR: MusicBrainz track import process');
    console.error(error.message);
    callback("Error importing MusicBrains tracks: " + error.message); 
  } 
}

//return true if this is an artist we will associate with a track in the database
function isArtistForDatabase(artistName) {
  return artistName.trim().length > 0 && artistName.trim() != 'Various Artists';
}