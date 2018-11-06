const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const importProgressSchema = new Schema({ 
  artist_import: {
    last_imported_id: {
      type: Number
    },
    last_imported_date: {
      type: Date
    }
  }
});

const ImportProgress = mongoose.model('ImportProgress', importProgressSchema);

module.exports = ImportProgress;
