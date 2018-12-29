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
  
  listens: [
    {
      _id: false,
      import_source: { type: String },
      listens: { type: Number }
    }
  ],

  created_at: {
    type: Date,
    default: Date.now
  },

  updated_at: {
    type: Date,
    default: Date.now
  }
});

artistSchema
  .index({"import_source.name": 1,"import_source.id": 1}, {"unique": true, "background": true})
  .index({"name": 1}, {"background": true});

const Artist = mongoose.model('Artist', artistSchema);

module.exports = Artist
