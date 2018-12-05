const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const importProgressSchema = new Schema({ 
  album_import: {
    last_imported_artist_id: {
      type: Number
    },
    last_imported_date: {
      type: Date
    },
    last_imported_release_group_id: {
      type: Number
    }
  },
  artist_import: {
    last_imported_id: {
      type: Number
    },
    last_imported_date: {
      type: Date
    }
  },
  artist_listen_import: {
    last_imported_index: {
      type: Number
    },
    last_imported_date: {
      type: Date
    }
  },
  track_import: {
    last_imported_artist_id: {
      type: Number
    },
    last_imported_date: {
      type: Date
    },
    last_imported_track_id: {
      type: Number
    }
  },
});

const ImportProgress = mongoose.model('ImportProgress', importProgressSchema);

module.exports = ImportProgress;
