package modules.pattern.manager;

import org.apache.log4j.Logger;


public class TFID {
	
	final static Logger logger = Logger.getLogger(TFID.class);
	
	/*
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("chspam").setMaster("local[2]")
				.set("spark.executor.memory", "1g")
				.set("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse");
		
		SparkContext sc = new SparkContext(conf);


		List<Row> data = Arrays.asList(
		  RowFactory.create(0.0, "Hi I heard about Spark"),
		  RowFactory.create(0.0, "I wish Java could use case classes"),
		  RowFactory.create(1.0, "Logistic regression models are neat")
		);
		StructType schema = new StructType(new StructField[]{
		  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
		  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
		});
		SparkSession spark = new SparkSession(sc);
		Dataset<Row> sentenceData = spark.createDataFrame(data, schema);
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		Dataset<Row> wordsData = tokenizer.transform(sentenceData);
		int numFeatures = 20;
		HashingTF hashingTF = new HashingTF()
		  .setInputCol("words")
		  .setOutputCol("rawFeatures")
		  .setNumFeatures(numFeatures);
		Dataset<Row> featurizedData = hashingTF.transform(wordsData);
		// alternatively, CountVectorizer can also be used to get term frequency vectors

		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		for (Row r : rescaledData.select("features", "label").takeAsList(3)) {
		  Vector features = r.getAs(0);
		  Double label = r.getDouble(1);
		  logger.info("FEATURES: " + features);
		  logger.debug("LABEL: " + label);
		}
	}
	*/

}