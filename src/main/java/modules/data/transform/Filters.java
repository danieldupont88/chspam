package modules.data.transform;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Filters {
	
	public static Function<Tuple2<List<List<String>>, Long>, Boolean> postProcessingFilter = new Function <Tuple2<List<List<String>>, Long>, Boolean> () {
		@Override
		public Boolean call(Tuple2<List<List<String>>, Long> v1) throws Exception {
			
			boolean returnVal = true;
			
			returnVal = v1._1.size() >= 6;
			Set<List<String>> set = new HashSet<List<String>>(v1._1);
			return returnVal && set.size() == v1._1.size();
		}
		
	};
	
	public static Function<Tuple2<String, List<String>>, Boolean> preProcessingFilter = new Function <Tuple2<String, List<String>>, Boolean> () {
		@Override
		public Boolean call(Tuple2<String, List<String>> v1) throws Exception {
			
			return !v1._2.toString().contains("Door");
			
		}
		
	};
	
	
}
