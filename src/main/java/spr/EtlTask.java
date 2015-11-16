package spr;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class EtlTask implements Callable<Void> {
	final String input;

	public EtlTask(String input) {
		super();
		this.input = input;
	}

	@Override
	public Void call() throws Exception {
		String fileText = this.getData();
		String capitalizedText = this.capitalize(fileText);
		String[] words = this.splitTextToArray(capitalizedText);
		Map<String, Integer> wordMap = this.createCount(words);
		this.generatePart1Output(capitalizedText);
		this.generatePart2Output(wordMap);
		return null;
	}

	public abstract String getData();
	
	public String capitalize(final String fileText){
		if(null!=fileText){
			return fileText.toUpperCase();
		}
		return null;
	}
	
	public String[] splitTextToArray(final String capitalizedFileText){
		String[] words = capitalizedFileText.split("[\\W]+");
		return words;
	}
	
	public Map<String, Integer> createCount(final String[] words){
		Map<String, Integer> wordMap =  new HashMap<>();
		for (String word : words) {
			Integer value = wordMap.get(word);
			if(null!=value){
				wordMap.put(word, ++value);
			}else{
				wordMap.put(word, Integer.valueOf(1));
			}
		}
		return wordMap;
	}
	
	public abstract void generatePart1Output(final String capitalizedText);
	
	public abstract void generatePart2Output(final Map<String, Integer> wordMap);

	
	
	
	
	
	

}
