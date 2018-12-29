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
    if (importProgress['album_import'] != null) {
      if (importProgress['album_import']['last_imported_artist_id'] != null) {
        lastImportedArtistId = importProgress['album_import']['last_imported_artist_id'];
      }
    } else {
      importProgress['album_import'] = { last_imported_artist_id: lastImportedArtistId };
    }
    
    // get artists from the database
    
    let numberOfReturnedRecords;
    let numberOfArtistsProcessed = 0;
    let numberOfAlbumsImported = 0;
    let albumLimit = 500;
    
    await pgClient.connect() //open postgresql connection to MusicBrainz database
    
    while (numberOfReturnedRecords == null || numberOfReturnedRecords > 0) {
      let artists = await db.Artist.find({
          "import_source.name": importSource, 
          "import_source.id":{"$gt":lastImportedArtistId}, 
          "listens.listens":{"$gt":10}
        }, 
        null,
        {
          skip: 0, 
          limit: lastImportedArtistId > 0 ? 500 : 50, //use 50 for the first batch, because "Various Artists" is the first record and it has a TON of albums
          sort: "import_source.id"
      });
      
      numberOfReturnedRecords = artists.length;
      
      // query the MusicBrainz database for albums by that artist
      
      console.log('Looking for albums for the artists: ' + artists.map(function(artist) { return artist['name'] + " (source id: " + artist['import_source']['id'] + ')'; }).join(", "));

      //loop through records in batches: this helps with artists that have an enormous number of albums, like "Various Artists"
      let parameters = artists.map(function(artist) { return artist['name'] });
      console.log('query start');
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
        'WHERE artist_credit.name IN (' + artists.map(function(artist, idx) { return '$' + (idx + 1); }).join(',') + ') ' +
        'ORDER BY release_group.id',
        parameters
      );
      
      console.log ('Found ' + res.rows.length + ' album records in MusicBrainz');
      
      let newAlbums = [];
      let idsToImport = [];
      
      await Promise.all(res.rows.map(async row => {
        if (row['first_release_id'] == null) {
          return;
        }
        idsToImport.push(row['first_release_id']);
        let newAlbum = new db.Album({
          name: row['album_name'],
          artist: row['artist_name'] == 'Various Artists' ? null : artists.filter(function(artist) {
            return artist['name'] == row['artist_name'];
          }),
          compilation: row['artist_name'] == 'Various Artists' ? true : false,
          label: row['label_name'],
          import_source: {
            'name': importSource,
            'id': row['first_release_id']
          }
        });
        newAlbums.push(newAlbum);
      }));
      
      console.log('looking for existing albums to prevent inserting duplicates...');
      let existingAlbums = await db.Album.find({"import_source.name":importSource,"import_source.id":{"$in":idsToImport}});
      let filteredAlbums = newAlbums.slice(0); //copy array so fitleredAlbums isn't created by reference
      let indexesToRemove = [];
      let existingImportSourceIds = [];
      for (let i = 0; i < existingAlbums.length; i++) {
        existingImportSourceIds.push(existingAlbums[i]['import_source']['id']);
      }
      for (let i = 0; i < filteredAlbums.length; i++) {
        if (existingImportSourceIds.indexOf(filteredAlbums[i]['import_source']['id']) != -1) {
          indexesToRemove.push(i);
        }
      }
      indexesToRemove.sort(function(a, b){return a-b});
      indexesToRemove = indexesToRemove.reverse();
      for (let i = 0; i < indexesToRemove.length; i++) {
        filteredAlbums.splice(indexesToRemove[i], 1);
      }
      console.log('finished looking for existing albums to prevent inserting duplicates (before: ' + newAlbums.length + ', after: ' + filteredAlbums.length + ')');
      
      if (filteredAlbums.length > 0) {
        console.log('inserting albums...');
        await db.Album.collection.insertMany(filteredAlbums);
        console.log('finished inserting albums');
        numberOfAlbumsImported += filteredAlbums.length;
      }
      
      await artists.map(function(artist) {
          lastImportedArtistId = Math.max(lastImportedArtistId, artist['import_source']['id']);
      });
      
      importProgress['album_import']['last_imported_artist_id'] = lastImportedArtistId;
      importProgress['album_import']['last_processed_date'] = Date.now();
      await importProgress.save();
      
      numberOfArtistsProcessed += artists.length;
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