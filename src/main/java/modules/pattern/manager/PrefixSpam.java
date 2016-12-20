package modules.pattern.manager;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpan.FreqSequence;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import org.bson.Document;

import com.mongodb.spark.api.java.MongoSpark;

import config.ChspamConfig;
import modules.data.transform.Filters;
import modules.data.transform.Mappers;
import scala.Tuple2;

public class PrefixSpam {

	final static Logger logger = Logger.getLogger(PrefixSpam.class);
	
	public static void processPattenrs() {
		
		logger.info("RUNNING PATTERN MINNING - 1 ");
		
		ChspamConfig config = ChspamConfig.getConfig();
		
		SparkConf conf = new SparkConf().setAppName("chspam").setMaster("local[2]")
				.set("spark.executor.memory", "1g")
				.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/chspam.context")
				.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/chspam.pattern");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Document> rdd = MongoSpark.load(sc);

		logger.info(rdd.first().toJson());
		
		logger.info("RUNNING PATTERN MINNING - 2 ");
		
		/*
		 * Fun��o que converte Documento
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
				if (loc != null){
					ctx.add("location:" + loc.entrySet().toString());
				}
				if (sit != null) {
					ctx.add("situation:"+ sit.entrySet().toString());
				}
				return new Tuple2(entId, ctx);
			}
		};
		 
		
		/* 
		 * Aplica a fun��o keyData
		 */
		JavaPairRDD<String, List<String>> contextPerUser = rdd.mapToPair(keyData);
		
		/*
		 * FILTROS DE PR� PROCESSAMENTO
		 */
		contextPerUser = contextPerUser.filter(Filters.preProcessingFilter);
		
		/*
		 * Agrega os contextos por entidade em uma lista por entidade
		 */
		JavaPairRDD<String, Iterable<List<String>>> reducedContextPeruser = contextPerUser.groupByKey();
		
		/*
		 * Transforma os contextos agregados por entidade em um RDD sem agrega��o
		 */
		JavaRDD<Iterable<List<String>>> contextsToMine = reducedContextPeruser.map( (t) -> t._2);
		
		JavaRDD<Iterable<List<String>>> contextsToMine2 = contextsToMine.map(Mappers.preMapJoinContext);
		
		
		/*
		 * Configura��o do algoritmo de descoberta de padr�o
		 */
		 
		PrefixSpan prefixSpan = new PrefixSpan().setMinSupport(config.getPatternMinSupport()).setMaxPatternLength(config.getPatternMaxLenght().intValue());
		PrefixSpanModel<String> model = prefixSpan.run(contextsToMine2);
		
		JavaRDD<FreqSequence<String>> freqSequences = model.freqSequences().toJavaRDD();
		
		JavaRDD<Tuple2<List<List<String>>, Long>> pattenrs = freqSequences.map( f -> new Tuple2(f.javaSequence(),  f.freq()));
		
		//FILTRO DE P�S PROCESSAMENTO
		JavaRDD<Tuple2<List<List<String>>, Long>> filterePattenrs = pattenrs.filter(Filters.postProcessingFilter); 
		
		//long numPattenrs = filterePattenrs.count();
		
		//logger.info("PATTENRS FOUND - 2 " + numPattenrs);
		
		//Limpa a base de padr�es da execu��o atual
		HistoryManager.clearThisExecutionCollection();	
		
		//Atualiza os hist�ricos de padr�es a partir dos padr�es descobertos
		filterePattenrs.foreach((p) -> HistoryManager.updateHistory(p));
		
		//Verifica o desaparecimento de um padr�o, a partir da ultima execu��o
		HistoryManager.checkPatternExtintion();
		
		//Limpa os padr�es encontrados na ultima execu��o 
		HistoryManager.clearLastExecutionCollection();		
		
		//Grava os padr�es atuais na base da ultima execu��o
		filterePattenrs.foreach((p) -> HistoryManager.storeLastPatternExecution(p));
		
		logger.info("RUNNING PATTERN MINNING - 2 - END ");
	}
}