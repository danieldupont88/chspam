package modules.history.comp;

import config.ChspamConfig;

public class DatabaseSource {

	public static void process() {

		ChspamConfig config = ChspamConfig.getConfig();

		HistoryComposer hc = new HistoryComposer();

		/*
		 * Se a importação não é incremental, deleta todos os registros de
		 * históricos anteriormente importados
		 */
		if (!ChspamConfig.getConfig().isIncrementalImport()) {
			hc.clearHistoryCollection();
		}

		importHistory();

	}
	public static void importHistory(){
		try {
			//Conectar ao banco, executar queries e converter para objetos Context
			//
			//salvar objetos Context usando a classe HistoryComposer
			//
			//HistoryComposer hc = new HistoryComposer();
			//hc.saveContext(c);
			
			/*
			while (dbCursor.hasNext()) {				
		
				Entity entity = new Entity("");
				Context c = new Context();
				c.setEntity(entity);
				
				List<String> contexts = Arrays.asList(StringUtils.split(splitedLine.get(1), ","));
				for (String context : contexts) {
					List<String> ctxComponent = Arrays.asList(StringUtils.split(context, ":"));
					
						Map<String, String> ctxMap = new HashMap<String, String>();
						ctxMap.put("PLACE", ctxComponent.get(1));
						
						if (ctxComponent.get(0).equals("LOCATION")) { 
							c.setLocation(ctxMap); 
						} else c.setSituation(ctxMap);						
				}
				
				HistoryComposer hc = new HistoryComposer();
				hc.saveContext(c);
				
			}*/
			
			
		}catch(Exception e){
			
		}
	}
}
