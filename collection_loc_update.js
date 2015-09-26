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
	var coor = doc.place.bounding_box.coordinates[0];
	col.update({_id : doc._id}, {$set : {"loc_point" : {"type": "Point", "coordinates": [(coor[0][0] + coor[2][0])/2, (coor[0][1] + coor[2][1])/2]}}});
}

