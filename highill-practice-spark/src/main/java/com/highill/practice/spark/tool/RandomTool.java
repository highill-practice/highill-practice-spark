package com.highill.practice.spark.tool;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomTool {
	
	public static int randomInt(int start, int end) {
		int randomInt = 0;
		if(end > start) {
			Random random = new Random();
			randomInt = random.nextInt(end - start) + start;
		}
		return randomInt;
	}
	
	public static List<Integer> randomIntList(int start, int end, int size) {
		List<Integer> randomIntList = null;
		if(size > 0 && end > start) {
			randomIntList = new ArrayList<Integer>();
			for(int randomSize = 0; randomSize < size; randomSize++) {
				int randomInt = randomInt(start, end);
				randomIntList.add(randomInt);
			}
		}
		
		return randomIntList;
	}

}
