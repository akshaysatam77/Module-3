package com.cg.appl.daos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.cg.appl.entities.Bill;
import com.cg.appl.entities.Consumer;
import com.cg.appl.exceptions.UserException;
import com.cg.appl.util.DbUtil;


public class UserMasterDaoImpl implements IUsermasterDao {

	private DbUtil util;
	
	public UserMasterDaoImpl(){
		
		util = new DbUtil();
	}
	

	
	@Override
	public Consumer getUserDetails(String consumerNumber) throws UserException {
		Connection conn = null;
		PreparedStatement pstm = null;
		ResultSet res=null;
		String query="select consumer_num,consumer_name,address from consumers where consumer_num=?";
		Consumer consume=new Consumer();
		try {
			conn=util.obtainConnection();
			pstm=conn.prepareStatement(query);
			pstm.setString(1,consumerNumber);
			res=pstm.executeQuery();
			while(res.next())
			{
				consume.setNumber(res.getString("consumer_num"));
				consume.setName(res.getString("consumer_name"));
				consume.setAddress(res.getString("address"));
				
			}
			return consume;
			/*else
			{
				
				throw new UserException("User number is wrong");
			}*/
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new UserException("JDBC failed,e");
		}finally
		{
			
			try {
				if(res!=null)
				{
					res.close();
				}
				if(pstm!=null)
				{
					pstm.close();
				}
				if(conn!=null)
				{
					conn.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new UserException("JDBC failed,e");
			}

		}
		
	}

	@Override
	public int setUserDetails(Bill bill) throws UserException {
		Connection conn = null;
		PreparedStatement pstm = null;
		int status=0;
		String query="INSERT INTO BILLDETAILS VALUES(SEQ_BILL_NUM.NEXTVAL,?,?,?,?,SYSDATE)";
		try {
			conn=util.obtainConnection();
			pstm=conn.prepareStatement(query);
			pstm.setString(1,bill.getConsumerNumber());
			pstm.setDouble(2, bill.getCurrReading());
			pstm.setDouble(3, bill.getUnitConsumed());
			pstm.setDouble(4,bill.getNetAmount());
			status=pstm.executeUpdate();
			
			
		} catch (UserException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new UserException("problem in insertion");
		}finally
		{
			try {
				if(pstm!=null)
				{
					pstm.close();
				}
				if(conn!=null)
				{
					conn.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	
		return status;
	}



	@Override
	public List<Consumer> showAll() throws UserException {
		Connection conn=null;
        PreparedStatement pstm=null;
        List<Consumer>myList=new ArrayList<>();
        ResultSet res=null;
        String query="SELECT CONSUMER_NUM,CONSUMER_NAME,ADDRESS FROM CONSUMERS";
        try {
        	conn=util.obtainConnection();
        	pstm=conn.prepareStatement(query);
        	res=pstm.executeQuery();
        	while(res.next())
        	{
        		Consumer consume=new Consumer();
        		consume.setNumber(res.getString("consumer_num"));
				consume.setName(res.getString("consumer_name"));
				consume.setAddress(res.getString("address"));
				
				myList.add(consume);
        		
        	}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new UserException("List failed to display");
		}
        
		return myList;
	}



	@Override
	public List<Bill> getConsumerDetails(String consumerNumber) throws UserException {
		Connection conn=null;
		PreparedStatement pstm=null;
		ResultSet res=null;
		String query="select bill_num,cur_reading,unitconsumed,netamount,bill_date from billdetails where consumer_num=?";
		 List<Bill> myBill=new ArrayList<>();
		
		try {
			conn=util.obtainConnection();
			pstm=conn.prepareStatement(query);
			pstm.setString(1,consumerNumber);
			res=pstm.executeQuery();
			while(res.next())
			{
				Bill bill=new Bill();
				bill.setBill_num(res.getInt("bill_num"));
				bill.setConsumerNumber(consumerNumber);
				bill. setCurrReading(res.getDouble("cur_reading"));
				bill.setUnitConsumed(res.getDouble("unitconsumed"));
				bill.setNetAmount(res.getDouble("netamount"));
				bill.setDate(res.getDate("bill_date"));
				myBill.add(bill);
				
			}
			return myBill;
		} catch (UserException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new UserException("JDBC FAILed,e");
		}
		
		
		
	}

}
