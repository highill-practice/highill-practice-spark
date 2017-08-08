package com.highill.practice.spark.tool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.highill.practice.spark.tool.bean.LibSVMLine;


@RunWith(BlockJUnit4ClassRunner.class)
public class LibSVMToolTest {
	
	@Test
	public void libSVMFile() {
		String fileName = "demo/data/libsvm/libsvm_test1.demo";
		File file = new File(fileName);
		if(file.exists()) {
			file.delete();
		}
		
		LibSVMLine line1 = LibSVMTool.libSVMLine(1, 100, 2, 3, 4, 5, 10, 20, 22);
		LibSVMLine line2 = LibSVMTool.libSVMLine(0, 50, 10, 9, 8, 7, 6, 5, 4, 3, 2, 11);
		LibSVMLine line3 = LibSVMTool.libSVMLine(0, 20, 10, 12, 13, 14, 10, 9, 8, 7, 6);
		LibSVMLine line4 = LibSVMTool.libSVMLine(1, 80, 2, 4, 6, 8, 10, 11, 20, 21, 22, 23);
		
		List<LibSVMLine> libSVMLineList = new ArrayList<LibSVMLine>();
		libSVMLineList.add(line1);
		libSVMLineList.add(line2);
		libSVMLineList.add(line3);
		libSVMLineList.add(line4);
		
		LibSVMTool.writeLibSVM(fileName, libSVMLineList);
		LibSVMTool.writeLibSVM(fileName, libSVMLineList, false);
		
		
		String fileName2 =  "demo/data/libsvm/libsvm_2_classes.demo";
		List<LibSVMLine> libSVM2ClassesList = LibSVMTool.randomLibSVMLineList(2, 100);
		LibSVMTool.writeLibSVM(fileName2, libSVM2ClassesList);
		
		String fileName4 = "demo/data/libsvm/libsvm_4_classes.demo";
		List<LibSVMLine> libSVM4ClassesList = LibSVMTool.randomLibSVMLineList(4, 200);
		LibSVMTool.writeLibSVM(fileName4, libSVM4ClassesList);
		
		
		
	}
}
