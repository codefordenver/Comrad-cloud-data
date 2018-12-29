const mongoose = require('mongoose')
const Schema = mongoose.Schema

const albumSchema = new Schema({
  name: {
    type: String
  },

  artist: {
    type: Schema.Types.ObjectId,
    ref: 'Artist'
  },

  label: {
    type: String
  },

  compilation: {
    type: Boolean
  },

  created_at: {
    type: Date,
    default: Date.now
  },

  updated_at: {
    type: Date,
    default: Date.now
  },
  
  import_source: { //eg, name: MusicBrainz, id: 123
    name: String, //the name of the external system this record came from
    id: Number //the id in the external system for this record
  }
})

albumSchema
  .index({"import_source.name": 1,"import_source.id": 1}, {"unique": true, "background": true});

const Album = mongoose.model('Album', albumSchema)

module.exports = Album
