package modules.pattern.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import org.apache.spark.mllib.fpm.PrefixSpan.FreqSequence;
import org.bson.Document;

import com.mongodb.spark.api.java.MongoSpark;

import pattern.HistoryManager;
import pattern.PatternHistory;
import scala.Tuple2;

public class PrefixSpamFromMongo {
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("chspam").setMaster("local[2]")
				.set("spark.executor.memory", "1g")
				.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/chspam.context")
				.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/chspam.pattern");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Document> rdd = MongoSpark.load(sc);

		System.out.println(rdd.first().toJson());
		
		/*
		 * Função que converte Documento
		 * para uma tupla <"ENTIDADE", "CONTEXTO1,CONTEXTO2,CONTEXTO3">
		 */
		PairFunction<Document, String, List<String>> keyData = new PairFunction<Document, String, List<String>>() {
			
			@Override
			public Tuple2<String, List<String>> call(Document arg0) throws Exception {
				Document ent = (Document) arg0.get("entity");
				String entId = ent.getString("id");
				
				Document loc = (Document) arg0.get("location");
				Document sit = (Document) arg0.get("situation");
				
				List<String> ctx = new ArrayList<>();
				ctx.add("location:" + loc.entrySet().toString());
				ctx.add("situation:"+ sit.entrySet().toString());
				
				return new Tuple2(entId, ctx);
			}
		};
		
		/* 
		 * Aplica a função keyData
		 */
		JavaPairRDD<String, List<String>> contextPerUser = rdd.mapToPair(keyData);
		
		/*
		 * Agrega os contextos por entidade em uma lista por entidade
		 */
		JavaPairRDD<String, Iterable<List<String>>> reducedContextPeruser = contextPerUser.groupByKey();
		
		/*
		 * Transforma os contextos agregados por entidade em um RDD sem agregação
		 */
		JavaRDD<Iterable<List<String>>> contextsToMine = reducedContextPeruser.map( (t) -> t._2);
		
		/*
		 * Configuração do algoritmo de busca de descoberta de padrão
		 */
		PrefixSpan prefixSpan = new PrefixSpan().setMinSupport(0.5).setMaxPatternLength(5);
		PrefixSpanModel<String> model = prefixSpan.run(contextsToMine);
		
		JavaRDD<FreqSequence<String>> freqSequences = model.freqSequences().toJavaRDD();
		
		JavaRDD<Tuple2<List<List<String>>, Long>> histories = freqSequences.map( f -> new Tuple2(f.javaSequence(),  f.freq()));
		
		VoidFunction<Tuple2<List<List<String>>, Long>> storeHistory = new VoidFunction<Tuple2<List<List<String>>, Long>>() {
			@Override
			public void call(Tuple2<List<List<String>>, Long> arg0) throws Exception {
				System.out.println("storing tuple: " + arg0.toString());
				HistoryManager.updateHistory(arg0);
			}
		};
		
		histories.foreach(storeHistory);
		
		HistoryManager.printSavedPatterns();
	}
}