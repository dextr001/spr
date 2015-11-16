package spr;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExtractorApp {

	public static void execute(final String fromDirectoryPath, final String toFirstPartDirectoryPath, final String toSecondPartDirectoryPath){
		ThreadPoolExecutor executor = new ThreadPoolExecutor(100, 500, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		long start = System.currentTimeMillis();
		List<String> fileNames = produceInput(fromDirectoryPath);
		long mid = System.currentTimeMillis();
		System.out.println("Time Taken for producing input: "+Double.valueOf((mid-start)/1000) + " seconds");
		for (String filePath : fileNames) {
			EtlTask readerTask =  new FileSystemEtlTask(filePath, toFirstPartDirectoryPath, toSecondPartDirectoryPath);
			executor.submit(readerTask);
		}
		long end = System.currentTimeMillis();
		System.out.println("Time Taken for creating tasks: "+Double.valueOf((end-start)/1000) + " seconds");
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			long end2 = System.currentTimeMillis();
			System.out.println("Time Taken till termination: "+Double.valueOf((end2-start)/1000) + " seconds");
		} catch (InterruptedException e) {
			System.err.println("ThreadPoolExecutor was interrupted");
			e.printStackTrace();
		}
	}
	private static List<String> produceInput(final String inputPath){
		List<String> fileNames = DirectoryReader.fileList(inputPath);
		return fileNames;
	}
}
