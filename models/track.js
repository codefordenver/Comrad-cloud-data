//suggested index:
//{"import_source.name": 1,"import_source.id": 1}

const mongoose = require('mongoose')
const Schema = mongoose.Schema

const trackSchema = new mongoose.Schema({
  name: {
    type: String
  },
  
  album: {
    type: Schema.Types.ObjectId,
    ref: 'Album'
  },
  
  artists: [
    {
      type: Schema.Types.ObjectId,
      ref: 'Artist'
    }
  ],
  
  duration_in_seconds: {
    type: Number
  },
  
  track_number: {
    type: Number
  },
  
  disk_number: {
    type: Number
  },
  
  import_source: { //eg, name: MusicBrainz, id: 123
    name: String, //the name of the external system this record came from
    id: Number //the id in the external system for this record
  },
  
  listens: [
    {
      import_source: { type: String },
      listens: { type: Number }
    }
  ],
  
  popularity: { //a number from 0-100, with 100 being the most popular
    type: Number
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

trackSchema
  .index({"import_source.name": 1,"import_source.id": 1}, {"unique": true, "background": true})
  .index({"name": 1,"artists": 1}, {"background": true})
  .index({"listens.listens": -1}, {"background": true});

const Track = mongoose.model('Track', trackSchema);

module.exports = Track
