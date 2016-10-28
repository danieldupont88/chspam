package pattern;

import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBList;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import scala.Tuple2;

public class HistoryManager {

	static MongoClient mongo = new MongoClient("localhost", 27017);
	static MongoDatabase db = mongo.getDatabase("chspam");
	static MongoCollection<Document> patternHistoryCollection = db.getCollection("pattern-history");

	public static void updateHistory(Tuple2<List<List<String>>, Long> pattern) {
		patternHistoryCollection.find();

		Document query = new Document("pattern", pattern._1);

		// Busca por um registro contendo o padrão
		Document findFirst = patternHistoryCollection.find(query).first();

		// Caso não encontre, cria um registro novo
		if (findFirst == null) {
			Document newPatternHistory = new Document();
			newPatternHistory.put("pattern", pattern._1);

			Document hist = new Document();
			hist.put("discoveredIn", new Date());
			hist.put("frequency", pattern._2.longValue());

			BasicDBList historyArray = new BasicDBList();
			historyArray.add(hist);
			newPatternHistory.put("history", historyArray);
			patternHistoryCollection.insertOne(newPatternHistory);
		}
		// Quando encontra, adiciona mais uma entrada na lista de histórico.
		else {

			Document hist = new Document();
			hist.put("discoveredIn", new Date());
			hist.put("frequency", pattern._2);

			Document pushElement = new Document("$push", hist);
			patternHistoryCollection.updateOne(query, pushElement);
		}

	}

	public static void printSavedPatterns() {

		FindIterable<Document> documents = patternHistoryCollection.find();

		documents.forEach(new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document);
			}
		});
	}
	
	public static void clearHistoryCollection() {
		patternHistoryCollection.drop();
	};

}
