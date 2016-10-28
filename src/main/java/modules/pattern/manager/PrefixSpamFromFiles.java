package modules.pattern.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpan.FreqSequence;
import org.apache.spark.mllib.fpm.PrefixSpanModel;

import pattern.PatternHistory;
import scala.Tuple2;

public class PrefixSpamFromFiles {
	public static void main(String[] args) {

		String contextFile = "C:/Dev/context/context-saple1.txt";

		String contextFile2 = "C:/Dev/context/context-saple2.txt";
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
				.set("spark.executor.memory", "1g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> linesRdd = sc.textFile(contextFile);
		
		JavaRDD<String> linesRdd2 = sc.textFile(contextFile2);

		/*
		 * Função que converte string no padrão ENTIDADE|CONTEXTO1,CONTEXTO2,CONTEXTO3
		 * para uma tupla <"ENTIDADE", "CONTEXTO1,CONTEXTO2,CONTEXTO3">
		 */
		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String x) {
				return new Tuple2(x.split(Pattern.quote("|"))[0], x.split(Pattern.quote("|"))[1]);
			}
		};
		
		/* 
		 * Aplica a função keyData
		 */
		JavaPairRDD<String, String> contextPerUser = linesRdd.mapToPair(keyData);
		JavaPairRDD<String, String> contextPerUser2 = linesRdd2.mapToPair(keyData);

		/*
		 * Reduz os dados para uma tupla
		 * <"ENTIDADE", "CONTEXTO1,CONTEXTO2,CONTEXTO3|CONTEXTO1,CONTEXTO2,CONTEXTO3">
		 * 
		 * */
		JavaPairRDD<String, String> a1 = contextPerUser.reduceByKey( (a,b) -> (a + "|" + b));
		JavaPairRDD<String, String> a2 = contextPerUser2.reduceByKey( (a,b) -> (a + "|" + b));
		
		
		//System.out.println(a1.collect());
		
		//JavaPairRDD<String, List<String>> a2 = a1.map( (a,b) -> new Tuple2(a, b._2.split(Pattern.quote(","))));
		
		/*
		 * Map convertendo a Tupla  <"ENTIDADE",  "CONTEXTO1,CONTEXTO2,CONTEXTO3|CONTEXTO1,CONTEXTO2,CONTEXTO3"> 
		 * para uma Tupla <"ENTIDADE", List<"CONTEXTO1","CONTEXTO2","CONTEXTO3">, List<"CONTEXTO1","CONTEXTO2","CONTEXTO3">
		 */
		JavaRDD<List<List<String>>> contextListPerUser = a1.map(
				new Function<Tuple2<String, String>, List<List<String>>>() {
					public List<List<String>> call(Tuple2<String, String> arg0) throws Exception {
						List<String> contexts = Arrays.asList(arg0._2.split(Pattern.quote("|")));
						List<List<String>> contextLists = new ArrayList<List<String>>();
						contexts.forEach(e -> contextLists.add(Arrays.asList(e.split(Pattern.quote(",")))));
						
						return contextLists;
					}
				});
		
		JavaRDD<List<List<String>>> contextListPerUser2 = a2.map(
				new Function<Tuple2<String, String>, List<List<String>>>() {
					public List<List<String>> call(Tuple2<String, String> arg0) throws Exception {
						List<String> contexts = Arrays.asList(arg0._2.split(Pattern.quote("|")));
						List<List<String>> contextLists = new ArrayList<List<String>>();
						contexts.forEach(e -> contextLists.add(Arrays.asList(e.split(Pattern.quote(",")))));
						
						return contextLists;
					}
				});

		//System.out.println(contextListPerUser.collect());
				
		//contextListPerUser.collect().forEach(c -> {System.out.println(c._1); System.out.println(c._2);});
		
		
		
		//contextListPerUser.groupBy(t -> t._1);
		
		/*JavaRDD<List<List<String>>> sequences =  sc.parallelize(
				Arrays.asList(Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c")),
						Arrays.asList(Arrays.asList("a"), Arrays.asList("c", "b"), Arrays.asList("a", "b")),
						Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("e")), Arrays.asList(Arrays.asList("f"))),
				2);
	 	*/
		
		//JavaPairRDD<List<List<String>>, List<PatternHistory>> history = new JavaPairRDD<>(rdd, kClassTag, vClassTag);
		
		
		PrefixSpan prefixSpan = new PrefixSpan().setMinSupport(0.5).setMaxPatternLength(5);
		PrefixSpanModel<String> model = prefixSpan.run(contextListPerUser);
		
		JavaRDD<FreqSequence<String>> freqSequences = model.freqSequences().toJavaRDD();
		
		JavaRDD<Tuple2<List<List<String>>, PatternHistory>> history = freqSequences.map( f -> new Tuple2(f.javaSequence(),  new PatternHistory(f.freq()) )  );
		
		PrefixSpanModel<String> model2 = prefixSpan.run(contextListPerUser2);
		JavaRDD<FreqSequence<String>> freqSequences2 = model2.freqSequences().toJavaRDD();
		
		JavaRDD<Tuple2<List<List<String>>, PatternHistory>> history2 = freqSequences2.map( f -> new Tuple2(f.javaSequence(),  new PatternHistory(f.freq()) )  );
		
		JavaRDD<Tuple2<List<List<String>>, PatternHistory>> historyFinal = history.union(history2);
		
		System.out.println(historyFinal.collect());
		/*for (FreqSequence<String> freqSeq : model.freqSequences().toJavaRDD().collect()) {
			
			new Tuple2(freqSeq.javaSequence(), freqSeq.freq()); 
			
			System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
		}*/
		
	}
}