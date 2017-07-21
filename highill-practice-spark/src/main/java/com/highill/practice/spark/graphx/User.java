package com.highill.practice.spark.graphx;

import java.io.Serializable;

public class User implements Serializable {

	/**
	 * 
	 */
    private static final long serialVersionUID = 1L;

	private Long userId;
	
	private String username;

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@Override
    public String toString() {
        return "User [userId=" + userId + ", username=" + username + "]";
    }
	
	

}
