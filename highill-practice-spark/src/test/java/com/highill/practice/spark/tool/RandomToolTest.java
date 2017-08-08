package com.highill.practice.spark.tool;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class RandomToolTest {
	
	@Test
	public void random() {
		List<Integer> intList1 = RandomTool.randomIntList(1, 10, 30);
		System.out.println("-----intList1: " + intList1);
		
		List<Integer> intList2 = RandomTool.randomIntList(100, 200, 30);
		System.out.println("-----intList2: " + intList2);
	}

}
