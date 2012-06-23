# This is an implementation of the OT data backend for mongodb.

mongodb  = require 'mongodb'
Db       = mongodb.Db
ObjectID = mongodb.ObjectID

defaultOptions = {
  # Inherit the default options from mongodb. (Hostname: 127.0.0.1, port: 27017)
  hostname: 'localhost'
  port: '27017'
  database: 'share'
  opsCollection: 'ops'
  snapshotCollection: 'snapshots'
}

# Valid options as above.

# module.exports = MongoDb = (options) ->
#   return new Db if !(this instanceof MongoDb)

class MongoDb

  options = @options
  _db = null
  _docId = null
  _collections = null

  constructor: (options) ->

    @options = options ? {}
    @options[k] ?= v for k, v of defaultOptions

  openDatabase: (callback) ->
    return callback null, @_db if @_db?
    Db.connect 'mongodb://' + @options.hostname + ':' + @options.port + '/' + @options.database, (err, db) =>
      callback err, @_db = db

  closeDatabase: (callback) ->
    return callback null if not @_db?
    @_db.close (err) =>
      @_db = null
      callback err

  openCollection: (collectionName, callback) ->
    return callback null, @_collection if @_collection[collectionName]?
    @openDatabase (err, db) =>
      db.collection collectionName, (err, collection) =>
        callback err, @_collection[collectionName] = collection

  find: (collectionName, query, callback) ->
    @openCollection collectionName, (err, collection) ->
      return callback err if err
      collection.find query, callback

  update: (collectionName, query, updates, callback) ->
    @openCollection collectionName, (err, collection) =>
      return callback err if err
      collection.update query, updates, {safe: true}, callback

  # Creates a new document.
  # data = {snapshot, type:typename, [meta]}
  # calls callback(true) if the document was created or
  #   callback(false) if a document with that name already exists.
  create: (docId, data, callback) ->

    @openCollection collectionName, (err, collection) =>
      return callback err if err

      @find @options.snapshotCollection, {docId: docId}, (err, cursor) =>
        cursor.toArray (err, result) ->
          if result.length == 0
            collection.insert
              docId: docId
              ops: [],
              data: data
            , {safe:true}, (err, doc) =>
              # console.log 'ERROR: ' , err, 'DOC:' , doc, "DOC CREATED"
              if (callback)
                return callback(err, doc)
          else
            if (callback)
              return callback('Document already exists')

  # Get all ops with version = start to version = end. Noninclusive.
  # end is trimmed to the size of the document.
  # If any documents are passed to the callback, the first one has v = start
  # end can be null. If so, returns all documents from start onwards.
  # Each document returned is in the form {op:o, meta:m, v:version}.
  getOps: (docId, start, end, callback) ->

    if start == end
      callback []
      return

    @find @options.opsCollection, {docId: docId}, (err, cursor) =>
      throw err if err?

      cursor.toArray (err, result) =>
        if result.length != 0

          doc = result[0]

          if (end == null || end < start)
            end = doc.ops.length

          results = []
          for v in [start..end]
            r = doc.ops[v]
            r.v = v
            results.push(op)
          return callback(results)


  # Write an op to a document.
  #
  # opData = {op:the op to append, v:version, meta:optional metadata object containing author, etc.}
  # callback = callback when op committed
  #
  # opData.v MUST be the subsequent version for the document.
  writeOp: (docId, opData, callback) ->

    # ****** NOT SAFE FOR MULTIPLE PROCESSES. Rewrite me using transactions or something.

    # The version isn't stored.

    json = JSON.stringify {op:opData.op, meta:opData.meta}

    @openCollection @options.opsCollection, (err, collection) =>
      return callback err if err
      collection.insert
        docId: docId
        op: opData.op,
        meta: opData.meta
      , {safe:true}, (err, doc) =>
        if (callback)
          return callback(err, doc)


  append: (docId, op_data, doc_data, callback) ->
    if (typeof doc_data.snapshot == 'undefined')
      throw new Error('snapshot missing from data')
    if (typeof doc_data.type == 'undefined')
      throw new Error('type missing from data')

    resultingVersion = op_data.v + 1
    if (doc_data.v != resultingVersion)
      throw new Error('version missing or incorrect in doc data')

    @update @options.opsCollection, {docId: docId}, {
      '$set':
        data: doc_data
      '$push':
        ops:
          op: op_data.op,
          meta: op_data.meta
    }, (err) ->
      throw err if err?
      if (callback)
        return callback()

  # Write new snapshot data to the database.
  #
  # data = resultant document snapshot data. {snapshot:s, type:t, meta}
  #
  # The callback just takes an optional error.
  #
  # This function has UNDEFINED BEHAVIOUR if you call append before calling create().
  writeSnapshot: (docId, data, meta, callback) ->
    @openCollection @options.snapshotCollection, (err, collection) =>
      return callback err if err
      collection.update {docId: docId}, {'$set': {'data': data}}, {safe: true}, callback


  # Data = {v, snapshot, type}. Snapshot == null and v = 0 if the document doesn't exist.
  getSnapshot: (docId, callback) ->
    @openCollection @options.snapshotCollection, (err, collection) =>
      return callback err if err
      collection.findOne {docId: docId}, (err, result) ->
        throw err if err?
        if result == null
          return callback('Document does not exist', null)
        else
          return callback(null, result.data)

  getVersion: (docId, callback) ->
    @find @options.snapshotCollection, {docId: docId}, (err, cursor) =>
      throw err if err?

      cursor.toArray (err, result) =>
        if result.length != 0

          doc = result[0]

          if doc != null
            return callback(doc.ops.length)
          else
            return callback(null)

  # Permanently deletes a document. There is no undo.
  # Callback takes a single argument which is true if something was deleted.
  delete: (docId, dbMeta, callback) ->
    @openCollection @options.snapshotCollection, (err, collection) =>
      return callback err if err
      collection.remove {docId: docId}, {safe: true}, (err, result) =>
        if result == 0
          callback 'Document does not exist'
        else
          callback err, result

  # Close the connection to the database
  close: ->
    # client.quit()

module.exports = MongoDb
