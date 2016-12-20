package modules.data.transform;

import java.util.List;

import org.apache.spark.api.java.function.Function;

public class Mappers {
	
	public static Function<Iterable<List<String>>, Iterable<List<String>>> preMapJoinContext = new Function<Iterable<List<String>>, Iterable<List<String>>>() {
		@Override
		public Iterable<List<String>> call(Iterable<List<String>> v1) throws Exception {

			for (List<String> l : v1) {
				String rtnStr = "";
				for (String s : l) {
					if (!rtnStr.equals("")) { 
						rtnStr = rtnStr.concat(",");
					}
					rtnStr = rtnStr.concat(s);
				}
				l.clear();
				l.add(rtnStr);
			}
			for (List<String> l : v1) {
				
				l.get(0);
			}
			
			return v1;
		}
	};
}
