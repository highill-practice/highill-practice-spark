package com.highill.practice.spark.tool.bean;

import java.util.TreeMap;

public class LibSVMLine {
	
	private Integer label;
	
	private TreeMap<Number, Number> indexValueMap;
	
	public LibSVMLine() {
		
	}
	
	public LibSVMLine(Integer label, TreeMap<Number, Number> indexValueMap) {
		this.label = label;
		this.indexValueMap = indexValueMap;
	}

	public Integer getLabel() {
		return label;
	}

	public void setLabel(Integer label) {
		this.label = label;
	}

	public TreeMap<Number, Number> getIndexValueMap() {
		return indexValueMap;
	}

	public void setIndexValueMap(TreeMap<Number, Number> indexValueMap) {
		this.indexValueMap = indexValueMap;
	}

	@Override
    public String toString() {
	    return "LibSVMLine [label=" + label + ", indexValueMap=" + indexValueMap + "]";
    }
	
	

}
