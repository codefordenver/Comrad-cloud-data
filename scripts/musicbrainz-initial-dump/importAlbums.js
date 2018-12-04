const db = require('../../models');
const { Client }  = require('pg');
const mongoose = require('mongoose');

const importSource = "MusicBrainz";

mongoose.connect(process.env.MONGO_CONNECTION);
const pgClient = new Client()

module.exports = async function(event, context, callback) {   
  console.log('Starting MusicBrainz album import process.');
  try {
    //Connect to MusicBrainz DB and run a query for all albums
    //Import MusicBrainz albums into a Mongo database
    
    let importProgress = await db.ImportProgress.findOne();
    if (importProgress == null) {
      throw "No importProgress record found, run artist import first to create that record.";
    }
    let lastImportedArtistId = 0;
    let lastImportedReleaseGroupId = 0;
    if (importProgress['album_import'] != null) {
      if (importProgress['album_import']['last_imported_artist_id'] != null) {
        lastImportedArtistId = importProgress['album_import']['last_imported_artist_id'];
      }
      if (importProgress['album_import']['last_imported_release_group_id'] != null) {
        lastImportedReleaseGroupId = importProgress['album_import']['last_imported_release_group_id'];
      }
    } else {
      importProgress['album_import'] = { last_imported_artist_id: lastImportedArtistId, lastImportedReleaseGroupId: 0 };
    }
    
    // get artists from the database
    
    let numberOfReturnedRecords;
    let numberOfArtistsProcessed = 0;
    let numberOfAlbumsImported = 0;
    let albumLimit = 500;
    
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
      
      console.log('Looking for albums for the artist: ' + artist['name'] + ' (source id: ' + artist['import_source']['id'] + ')');
      
      let numberOfAlbumRecords;

      //loop through records in batches: this helps with artists that have an enormous number of albums, like "Various Artists"
      while (typeof numberOfAlbumRecords == 'undefined' || numberOfAlbumRecords > 0) { 
        if (lastImportedReleaseGroupId > 0) {
          console.log("lastImportedReleaseGroupId: " + lastImportedReleaseGroupId);
        }
        let res = await pgClient.query(
          'SELECT artist_credit.name AS artist_name, release_group.name AS album_name, release_group.id AS release_group_id, ' +
          '( ' + 
          'SELECT release.id FROM release ' + 
            'LEFT JOIN release_country ON release_country.release = release.id ' + 
            'LEFT JOIN release_label ON release.id = release_label.release ' + 
            'LEFT JOIN musicbrainz.label ON release_label.label = label.id ' + 
          'WHERE release.release_group = release_group.id ' + 
          'ORDER BY date_year, date_month, date_day ' + 
          'LIMIT 1) AS first_release_id, ' + 
          '( ' + 
          'SELECT label.name FROM musicbrainz.release ' + 
            'LEFT JOIN release_country ON release_country.release = release.id ' + 
            'LEFT JOIN release_label ON release.id = release_label.release ' + 
            'LEFT JOIN musicbrainz.label ON release_label.label = label.id ' + 
          'WHERE release.release_group = release_group.id ' + 
          'ORDER BY date_year, date_month, date_day ' + 
          'LIMIT 1) AS label_name ' + 
          'FROM release_group  ' + 
          'INNER JOIN artist_credit ON artist_credit.id = release_group.artist_credit ' +
          'WHERE artist_credit.name = $1 ' + 
          'AND release_group.id > $2 ' +
          'ORDER BY release_group.id ' +
          'LIMIT $3', 
          [artist['name'], lastImportedReleaseGroupId, albumLimit]
        );
        
        let newAlbums = [];
        let idsToImport = [];
        
        numberOfAlbumRecords = res.rows.length;
        
        await Promise.all(res.rows.map(async row => {
          lastImportedReleaseGroupId = Math.max(lastImportedReleaseGroupId, row['release_group_id']);
          idsToImport.push(row['first_release_id']);
          let newAlbum = new db.Album({
            name: row['album_name'],
            artist: artist['name'] == 'Various Artists' ? null : artist,
            compilation: artist['name'] == 'Various Artists' ? true : false,
            label: row['label_name'],
            import_source: {
              'name': importSource,
              'id': row['first_release_id']
            }
          });
          newAlbums.push(newAlbum);
        }));
        
        let existingAlbums = await db.Album.find({"import_source.name":importSource,"import_source.id":{"$in":idsToImport}});
        let filteredAlbums = newAlbums.filter(function(newAlbum) {
        for (let i = 0; i < existingAlbums.length; i++) {
            if (existingAlbums[i]['import_source']['id'] == newAlbum['import_source']['id']) {
              return false;
            }
          }
          return true;
        });
        
        if (filteredAlbums.length > 0) {
          await db.Album.collection.insertMany(filteredAlbums);
          numberOfAlbumsImported += filteredAlbums.length;
        }
        
        importProgress['album_import']['last_imported_release_group_id'] = lastImportedReleaseGroupId;
        await importProgress.save();
      }
      
      lastImportedArtistId = Math.max(lastImportedArtistId, artist['import_source']['id']);
      
      lastImportedReleaseGroupId = 0;
      importProgress['album_import']['last_imported_release_group_id'] = lastImportedReleaseGroupId;
      importProgress['album_import']['last_imported_artist_id'] = lastImportedArtistId;
      importProgress['album_import']['last_processed_date'] = Date.now();
      await importProgress.save();
      
      numberOfArtistsProcessed++;
      
    }
        
    await pgClient.end(); //close the postgresql connection
    
    console.log('Finished MusicBrainz album import process: ' + numberOfAlbumsImported + ' albums from ' + numberOfArtistsProcessed + ' artists were imported');
    
    callback(null, 'Album import success: ' + numberOfAlbumsImported + ' albums from ' + numberOfArtistsProcessed + ' artists were imported');
    
  } catch (error) {
    console.error('ERROR: MusicBrainz album import process');
    console.error(error.message);
    callback("Error importing MusicBrains albums: " + error.message); 
  } 
}