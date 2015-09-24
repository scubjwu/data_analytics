//usage:
//mongo --eval "var DB_name='xx', col_target_name='xx', col_src_name='xx'"
var conn = new Mongo();

//put the database name below
var db = conn.getDB(DB_name);
var count = 0;

//put the collection name that you want to merge to
var col_target = db.getCollection(col_target_name)
count = col_target.count();
print(count);

col_target.createIndex({user_id : 1}, unique=true);

//put the collection name that you want to merge from
var col_src = db.getCollection(col_src_name)
var cursor = col_src.find();
while (cursor.hasNext()) {
	col_target.insert(cursor.next());
}

count = col_target.count();
print(count)
