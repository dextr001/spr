package spr;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class FileSystemEtlTask extends EtlTask{
	private final Path path;
	private final String toFirstPartDirectoryPath;
	private final String toSecondPartDirectoryPath;
	public FileSystemEtlTask(String input, String toFirstPartDirectoryPath, String toSecondPartDirectoryPath) {
		super(input);
		this.path = Paths.get(input);
		this.toFirstPartDirectoryPath = toFirstPartDirectoryPath;
		this.toSecondPartDirectoryPath = toSecondPartDirectoryPath;
	}
	
	@Override
	public String getData(){
		StringBuilder builder =  new StringBuilder();
		try (BufferedReader reader = Files.newBufferedReader(path)) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				builder.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder.toString();
	}
	
	@Override
	public void generatePart1Output(final String fileText){
		String fileName = path.getFileName().toString();
		Path toPath = Paths.get(toFirstPartDirectoryPath+"\\"+fileName);
		byte data[] = fileText.getBytes();
		try (OutputStream out = new BufferedOutputStream(
				Files.newOutputStream(toPath, StandardOpenOption.CREATE))) {
			out.write(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void generatePart2Output(final Map<String, Integer> wordMap){
		String fileName = path.getFileName().toString();
		Path toPath = Paths.get(toSecondPartDirectoryPath+"\\"+fileName);
		try (OutputStream out = new BufferedOutputStream(
				Files.newOutputStream(toPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
			for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
				String output = entry.getKey() + " -> " +entry.getValue() + "\n";
				byte data[] = output.getBytes();
				out.write(data);
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
