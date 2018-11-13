//suggested index:
//{"import_source.name": 1,"import_source.id": 1}


const mongoose = require('mongoose')
const Schema = mongoose.Schema

const artistSchema = new mongoose.Schema({
  name: {
    type: String
  },

  import_source: { //eg, name: MusicBrainz, id: 123
    name: String, //the name of the external system this record came from
    id: Number //the id in the external system for this record
  },

  created_at: {
    type: Date,
    default: Date.now
  },

  updated_at: {
    type: Date,
    default: Date.now
  }
});

const Artist = mongoose.model('Artist', artistSchema);

module.exports = Artist
