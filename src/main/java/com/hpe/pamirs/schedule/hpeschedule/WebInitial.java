package com.hpe.pamirs.schedule.hpeschedule;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;


public class WebInitial extends HttpServlet{

	private static final long serialVersionUID = 1L;
	public void init() throws ServletException{
		super.init();
		try {
			ConsoleManager.initial();
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
