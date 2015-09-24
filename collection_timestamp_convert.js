//usage:
//mongo --eval "var DB_name='xx', col_name='xx'"
var conn = new Mongo();

//put the database name below
var db = conn.getDB(DB_name);

//put the collection name that you want to operate
var col = db.getCollection(col_name);

var cursor = col.find();

while(cursor.hasNext()) {
	var doc = cursor.next();
	var time = new Date(doc.created_at);
	col.update({_id : doc._id}, {$set : {"created_at" : time}});
}

