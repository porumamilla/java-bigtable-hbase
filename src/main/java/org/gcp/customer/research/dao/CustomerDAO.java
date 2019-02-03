package org.gcp.customer.research.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.gcp.research.customer.model.Customer;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;

public class CustomerDAO {
	
	private Connection connection = null;
	public CustomerDAO(String projectId, String instanceId) throws Exception {
		Configuration conf = BigtableConfiguration.configure(projectId, instanceId);
		connection = BigtableConfiguration.connect(conf);
		
	}
	
	public void createCustomer(Customer customer) throws Exception {
		Table table = connection.getTable(TableName.valueOf("profile"));
		Put put = new Put(Bytes.toBytes(customer.getEmail()));
		put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("firstName"), Bytes.toBytes(customer.getFirstName()));
		put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("lastName"), Bytes.toBytes(customer.getLastName()));
		put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("address"), Bytes.toBytes(customer.getAddress()));
		table.put(put);
	}
	
	public void deleteCustomer(Customer customer) throws Exception {
		Table table = connection.getTable(TableName.valueOf("profile"));
		Delete delete = new Delete(Bytes.toBytes(customer.getEmail()));
	      // deleting the data
	    table.delete(delete);
	}
	
	public Customer getCustomerByEmail(String email) throws Exception {
		Table table = connection.getTable(TableName.valueOf("profile"));
		Result getResult = table.get(new Get(Bytes.toBytes(email)));
		Customer customer = new Customer();
		customer.setEmail(email);
		customer.setFirstName(Bytes.toString(getResult.getValue(Bytes.toBytes("name"), Bytes.toBytes("firstName"))));
		customer.setLastName(Bytes.toString(getResult.getValue(Bytes.toBytes("name"), Bytes.toBytes("lastName"))));
		customer.setAddress(Bytes.toString(getResult.getValue(Bytes.toBytes("address"), Bytes.toBytes("address"))));
		return customer;
	}
	
	public void createTable() throws Exception {
		Admin admin = connection.getAdmin();
		HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("profile"));
		descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("name")));
		descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("address")));
		admin.createTable(descriptor);
	}
}
