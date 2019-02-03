package com.gcp.research.customer.controller;

import org.gcp.customer.research.dao.CustomerDAO;

import com.gcp.research.customer.model.Customer;

public class CustomerController {

	public static void main(String[] args) throws Exception {
		CustomerDAO customerDAO = new CustomerDAO("test", "test");
		customerDAO.createTable();
		Customer customer = new Customer();
		customer.setEmail("porumamilla_raghu@yahoo.com");
		customer.setFirstName("Raghu");
		customer.setLastName("Porumamilla");
		customer.setAddress("7 Arbor Cir #727 Cincinnati OH 45255");
		customerDAO.createCustomer(customer);
		
		Customer customer1 = customerDAO.getCustomerByEmail("porumamilla_raghu@yahoo.com");
		System.out.println(customer1);
		customerDAO.deleteCustomer(customer1);
	}
}
