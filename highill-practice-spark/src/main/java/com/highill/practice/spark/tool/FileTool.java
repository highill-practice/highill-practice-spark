package com.highill.practice.spark.tool;

import java.io.File;

public class FileTool {
	
	public static void deletePath(String path) {
		File file = new File(path);
		deleteFile(file);
	}
	
	public static void deleteFile(File file) {
		if(file != null) {
			if(file.isDirectory()) {
				File[] childrenFileArray = file.listFiles();
				if(childrenFileArray != null && childrenFileArray.length > 0) {
					for(File childrenFile : childrenFileArray) {
						deleteFile(childrenFile);
					}
				}
			}
			
			file.delete();
		}
	}
}
