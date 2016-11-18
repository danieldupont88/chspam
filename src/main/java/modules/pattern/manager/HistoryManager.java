package modules.pattern.manager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBList;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import domain.AlertManager;
import scala.Tuple2;

public class HistoryManager {

	static MongoClient mongo = new MongoClient("localhost", 27017);
	static MongoDatabase db = mongo.getDatabase("chspam");
	static MongoCollection<Document> patternHistoryCollection = db.getCollection("pattern-history");
	
	static MongoCollection<Document> lastCollectedPatternCollection = db.getCollection("last-pattern");
	static MongoCollection<Document> thisCollectedPatternCollection = db.getCollection("this-pattern");
	
	static AlertManager alertManager = new AlertManager();
	
	public static void updateHistory(Tuple2<List<List<String>>, Long> pattern) {
		
		//armazena cada um dos padrões na base de execução atual
		storeThisExecutionPatterns(pattern);
		
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
			hist.put("type", "DISCOVERY");
			
			BasicDBList historyArray = new BasicDBList();
			historyArray.add(hist);
			newPatternHistory.put("history", historyArray);
			patternHistoryCollection.insertOne(newPatternHistory);
			
			//verifica se há alertas a serem gerados
			alertManager.verifyAlert(hist, newPatternHistory.getObjectId("_id").toHexString());
		}
		// Quando encontra, adiciona mais uma entrada na lista de histórico.
		else {
			
			Document hist = new Document();
			hist.put("discoveredIn", new Date());
			hist.put("frequency", pattern._2);
			
			List findFirstHistory = (ArrayList) findFirst.get("history");
			Document lastHistory =  (Document) findFirstHistory.get(findFirstHistory.size() -1);
			
			if ((Long) lastHistory.get("frequency") > pattern._2 ) {
				hist.put("type", "REDUCTION");
			} else if ((Long) lastHistory.get("frequency") > pattern._2) {
				hist.put("type", "GROWTH");
			} else {
				hist.put("type", "NO_CHANGE");
			}
			
			Document pushElement = new Document("$push", hist);
			patternHistoryCollection.updateOne(query, pushElement);
			//verifica se há alertas a serem gerados
			alertManager.verifyAlert(hist, findFirst.getObjectId("_id").toHexString());
		}

	}
	
	private static void storeThisExecutionPatterns(Tuple2<List<List<String>>, Long> pattern) {
		
		Document newPattern = new Document();
		newPattern.put("pattern", pattern._1);

		newPattern.put("frequency", pattern._2.longValue());
		
		thisCollectedPatternCollection.insertOne(newPattern);
	}
	
	public static void checkPatternExtintion() {
		/*
		 * Procura padrões da execução anterior na execução atal,
		 * caso não encontre, cria um registro de histórico com frequência 0
		 */
		FindIterable<Document> documents = lastCollectedPatternCollection.find();
		for (Document doc : documents) {

			Document query = new Document("pattern", doc.get("pattern"));
			Document findFirst = thisCollectedPatternCollection.find(query).first();
			
			if (findFirst == null) {
				
				Document hist = new Document();
				hist.put("discoveredIn", new Date());
				hist.put("frequency", 0);
				hist.put("type", "EXTINTION");

				Document pushElement = new Document("$push", hist);
				patternHistoryCollection.updateOne(query, pushElement);
				
				//verifica se há alertas a serem gerados
				alertManager.verifyAlert(hist, findFirst.getObjectId("_id").toHexString());
			}
					
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
	
	public static void clearLastExecutionCollection() {
		lastCollectedPatternCollection.drop();
	};
	
	public static void storeLastPatternExecution(Tuple2<List<List<String>>, Long> pattern) {
		Document newPatternHistory = new Document();
		newPatternHistory.put("pattern", pattern._1);

		Document hist = new Document();
		hist.put("discoveredIn", new Date());
		hist.put("frequency", pattern._2.longValue());

		newPatternHistory.put("history", hist);
		lastCollectedPatternCollection.insertOne(newPatternHistory);
	}

	public static void clearThisExecutionCollection() {
		thisCollectedPatternCollection.drop();
		
	}
	
}
