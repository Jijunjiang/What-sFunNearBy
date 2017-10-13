package db.mongodb;

import java.text.ParseException;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

public class MongoDBTableCreation {
	// run as java app to create MongoDB tanle with index
	public static void main(String[] args) throws ParseException {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase db = mongoClient.getDatabase(MongoDBUtil.DB_NAME);
		
		// step 1: remove old tables
		db.getCollection("users").drop();
		db.getCollection("items").drop();
		
		// step2: create new tables, populate data and create index
		db.getCollection("users")
			.insertOne(new Document().append("first_name", "john").append("last_name", "Smith")
					.append("password", "3229c1097c00d497a0fd282d586be050").append("user_id", "1111"));
		// make sure user_id is unique
		IndexOptions indexOptions = new IndexOptions().unique(true);
		
		// use 1 for ascending index, -1 for descending index
		// Different to MySQL, users table in MongoDB also has history info
		db.getCollection("users").createIndex(new Document("user_id", 1), indexOptions);
		db.getCollection("items").createIndex(new Document("item_id", 1), indexOptions);
		  // use a compound text index of name, address, etc. for search.
	    db.getCollection("items").createIndex(new Document().append("description", "text")
	        .append("snippet", "text").append("address", "text").append("name", "text"));

	    mongoClient.close();
	    System.out.println("Import is done successfully.");
	}
}
