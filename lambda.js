module.exports = {
  importAlbums:   require('./scripts/musicbrainz-initial-dump/importAlbums'),
  importArtists:  require('./scripts/musicbrainz-initial-dump/importArtists'),
  importListenBrainzArtistListens:  require('./scripts/musicbrainz-initial-dump/importListenBrainzArtistListens'),
  importListenBrainzTrackListens:  require('./scripts/musicbrainz-initial-dump/importListenBrainzTrackListens'),
  importTracks:  require('./scripts/musicbrainz-initial-dump/importTracks'),
  calculateInitialPopularity: require('./scripts/musicbrainz-initial-dump/calculateInitialPopularity'),
  generateDataDump: require('./scripts/musicbrainz-initial-dump/generateDataDump')
}