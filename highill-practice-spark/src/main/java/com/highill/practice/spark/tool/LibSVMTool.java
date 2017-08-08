package com.highill.practice.spark.tool;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.highill.practice.spark.tool.bean.LibSVMLine;

public class LibSVMTool {
	
	
	public static List<LibSVMLine> randomLibSVMLineList(int numClasses, int lineSize) {
		List<LibSVMLine> libSVMLineList = null;
		if(numClasses > 0 && lineSize > 0) {
			libSVMLineList = new ArrayList<LibSVMLine>();
			for(int line = 0; line < lineSize; line++) {
				Integer label = line % numClasses;
				Integer startIndex = RandomTool.randomInt(10, 1000);
				Integer lineValueSize = RandomTool.randomInt(20, 30);
				List<Integer> valueList = RandomTool.randomIntList(100, 1000, lineValueSize);
				LibSVMLine libSVMLine = libSVMLine(label, startIndex, valueList.toArray(new Number[0]));
				libSVMLineList.add(libSVMLine);
			}
		}
		return libSVMLineList;
	}
	
	public static LibSVMLine libSVMLine(Integer label, TreeMap<Number, Number> indexValueMap) {
		LibSVMLine libSVMLine = null;
		if(label != null && indexValueMap != null && !indexValueMap.isEmpty()) {
			libSVMLine = new LibSVMLine(label, indexValueMap);
		}
		return libSVMLine;
	}
	
	public static LibSVMLine libSVMLine(Integer label, Number startIndex, Number ... valueArray) {
		LibSVMLine libSVMLine = null;
		if(label != null && startIndex != null && valueArray != null && valueArray.length > 0) {
			TreeMap<Number, Number> indexValueMap = new TreeMap<Number, Number>();
			long index = startIndex.longValue();
			for(Number value : valueArray) {
				indexValueMap.put(index, value);
				index++;
			}
			
			libSVMLine = new LibSVMLine(label, indexValueMap);
		}
		return libSVMLine;
	}
	
	public static void writeLibSVM(String fileName, List<LibSVMLine> libSVMLineList) {
		writeLibSVM(fileName, libSVMLineList, false);
	}
	
	public static void writeLibSVM(String fileName, List<LibSVMLine> libSVMLineList, boolean append) {
		if(fileName != null && libSVMLineList != null && !libSVMLineList.isEmpty()) {
			try {
				File file = new File(fileName);
				if(!file.exists()) {
					file.getParentFile().mkdirs();
					append = false;
				}
				FileWriter fileWriter = new FileWriter(file, append);
				PrintWriter printWriter = new PrintWriter(fileWriter);
				
				List<String> lineList = stringLineList(libSVMLineList);
				if(lineList != null && !lineList.isEmpty()) {
					for(String line : lineList) {
						printWriter.print(line);
					}
				}
				
				printWriter.flush();
				fileWriter.flush();
				printWriter.close();
				fileWriter.close();
			} catch(Exception e) {
				System.out.println(e);
			}
		}
	}
	
	public static List<String> stringLineList(List<LibSVMLine> libSVMLineList) {
		List<String> lineList = null;
		if(libSVMLineList != null && !libSVMLineList.isEmpty()) {
			lineList = new ArrayList<String>();
			for(LibSVMLine libSVMLine : libSVMLineList) {
				String line = stringLine(libSVMLine);
				if(line != null && !line.isEmpty()) {
					lineList.add(line);
				}
			}
		}
		return lineList;
	}
	
	public static String stringLine(LibSVMLine libSVMLine) {
		String line = null;
		if(libSVMLine != null) {
			Integer label = libSVMLine.getLabel();
			TreeMap<Number, Number> indexValueMap = libSVMLine.getIndexValueMap();
			
			
			if(label != null && indexValueMap != null && !indexValueMap.isEmpty()) {
				StringBuffer lineBuffer = new StringBuffer();
				lineBuffer.append(label).append(" ");
				int indexSize = 1;
				for(Number index : indexValueMap.keySet()) {
					Number value = indexValueMap.get(index);
					if(index != null && value != null) {
						lineBuffer.append(index).append(":").append(value);
						if(indexSize < indexValueMap.size()) {
							lineBuffer.append(" ");
						}
					}
					indexSize++;
				}
				line = lineBuffer.toString();
				if(line != null) {
					line += "\r\n";
				}
			}
		}
		return line;
	}

}
