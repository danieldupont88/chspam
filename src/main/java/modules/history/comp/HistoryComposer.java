package modules.history.comp;

import java.util.Map;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import context.Context;
import context.Entity;

public class HistoryComposer {
	
	MongoClient mongo = new MongoClient( "localhost" , 27017 );
	MongoDatabase db = mongo.getDatabase("chspam");
	MongoCollection<Document> contextCollection = db.getCollection("context");

	public void clearHistoryCollection() {
		contextCollection.drop();
	};
	
	public void saveContext(Context context) {
		
		Document contextDocument = new Document();
		contextDocument.put("entity", parseEntityDocument(context.getEntity()));
		if (context.getLocation()!= null && !context.getLocation().isEmpty()) {
			contextDocument.put("location", parseContextualInfoDocument(context.getLocation()));
		}
		if ( context.getSituation() != null && !context.getSituation().isEmpty()) {
			contextDocument.put("situation", parseContextualInfoDocument(context.getSituation()));
		}
		contextCollection.insertOne(contextDocument);		
	}
	
	private Document parseEntityDocument(Entity ent) {
		
		Document entityDocument = new Document();
		entityDocument.put("id", ent.getId());
		entityDocument.put("name", ent.getName());
		
		return entityDocument;
	}
	
	public void printSavedContexts() {
		FindIterable<Document> documents = contextCollection.find();
		
		documents.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	private Document parseContextualInfoDocument(Map<String, String> ctx) {
		
		Document contextInfoDoc = new Document();
		for( Map.Entry<String, String> entry : ctx.entrySet()) {
			contextInfoDoc.put(entry.getKey(), entry.getValue());
		}
		
		return contextInfoDoc;
	}
			
}