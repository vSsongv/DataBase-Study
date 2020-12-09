/*
 DataBase ASM02 Creating Trigger, View and Operating with some Queries
 Song Jinyoung 201721747 Software and Engineering
*/

package DB2;

import java.util.*;
import java.sql.*;

public class SqlTest2 {
	
	static Connection conn;
	static Statement stm;
    static ResultSet r = null;
	
	public static ResultSet getQuery(String Query) throws SQLException
	{
		ResultSet r = stm.executeQuery(Query); // insert query
	    return r;
	}

	public static void main(String[] args) throws SQLException {
        
        String host = "localhost";
        String port = "5432";
        String db_name = "postgres";
        String username = "postgres";
        String password = "1745";
        int num = 0;
        
        Scanner scan = new Scanner(System.in);
		System.out.println("*******201721747 Song Jinyoung*******\n");
		System.out.println("SQL Programming Test");
		
		System.out.println("Connecting PostgreSQL database");
		conn = DriverManager.getConnection("jdbc:postgresql://"+host+":"+port+"/"+db_name+"",""+username+"",""+password);//Connect PostgreSQL
					
		try {
			
			if(conn != null)
			{
				System.out.println("Connection is good\n"); //Success to Connect
			}
			else System.out.println("Connection failed\n"); //Fail to Connect
			
			stm = conn.createStatement();
			
			System.out.println("<Trigger test 1>");
					
			String TQuery1 = "create or replace function trigger1() returns trigger as $$\r\n" + 
					"begin\r\n" + 
					"insert into Apply values(New.sID, 'Stanford', 'geology', null);\r\n" + 
					"insert into Apply values(New.sID, 'MIT', 'biology', null);\r\n" + 
					"return New;\r\n" + 
					"end;\r\n" + 
					"$$\r\n" + 
					"language 'plpgsql';\r\n" + 
					"create trigger R1\r\n" + 
					"after insert on Student\r\n" + 
					"for each row\r\n" + 
					"when (New.GPA > 3.3 and New.GPA <= 3.6)\r\n" + 
					"execute procedure trigger1();"; 
			stm.executeUpdate(TQuery1); // Create Trigger1
			System.out.println("Trigger R1 is created\n");
			
			String InsertQuery = "insert into Student values ('111', 'Kevin', 3.5, 1000);"
					+ "insert into Student values ('222', 'Lori', 3.8, 1000)";
			stm.executeUpdate(InsertQuery); //Insert Data
			System.out.println("Data is inserted.\n");
		
			String Query1 = "(select * from Student)"; //Display
			r = getQuery(Query1);
			System.out.println("<Result>");
			System.out.println("[sID]\t"+"[sName]\t"+"[GPA]\t"+"[sizeHS]"); //Display
			while(r.next())
			{
				System.out.println
				(
					r.getString(1)+" "+
					r.getString(2)+"\t"+
					r.getString(3)+"\t"+
					r.getString(4)
				);
			}
			System.out.println("\n");
			Query1 = "(select * from Apply)";
			r = getQuery(Query1);
			System.out.println("[sID]\t"+"[cName]\t"+"[major]\t"+"[decision]"); //Display
			while(r.next())
			{
				System.out.println
				(
						r.getString(1)+"\t"+
						r.getString(2)+"\t"+
						r.getString(3)+"\t"+
						r.getString(4)
				);
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<Trigger test 2>");
			
				String TQuery2 = "create or replace function trigger3() returns trigger as $$\r\n" + 
						"begin\r\n" + 
						"Update Apply\r\n" + 
						"set cName = new.cName\r\n" +
						"where cName = Old.cName;\r\n"+
						"return New;\r\n"+
						"end;\r\n" +
						"$$\r\n"+
						"language 'plpgsql';\r\n" + 
						"create trigger R3\r\n" + 
						"after Update of cName on College\r\n" + 
						"for each row\r\n" + 
						"execute procedure trigger3();"; 
				stm.executeUpdate(TQuery2); //create Trigger2
				System.out.println("Trigger R3 is created\n");
				
				String UpdateQuery = "Update College set cName = 'the Farm' where cName = 'Stanford';"
						+ "Update College set cName = 'Bezerkeley' where cName = 'Berkeley'";
				stm.executeUpdate(UpdateQuery); //Update Data
				System.out.println("Table is Updated.\n");
				
				String Query2 = "(select * from College)";
				r = getQuery(Query2);
				System.out.println("<Result>");
				System.out.println("[cName]\t"+"[state] "+"[enrollment]"); //Display
				while(r.next())
				{
					System.out.println
					(
						r.getString(1)+" "+
						r.getString(2)+"\t"+
						r.getString(3)
					);
				}
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<View test 1>");
				
				String VQuery = "create view CSaccept as\r\n" + 
						"select sID, cName\r\n" + 
						"from Apply\r\n" + 
						"where major = 'CS' and decision = 'Y'";
				stm.executeUpdate(VQuery); //create View
				System.out.println("View CSaccept is created\n");
				
				String Query3 = "select * from CSaccept";				
				r = getQuery(Query3);
				System.out.println("<Result>");
				System.out.println("[sID]\t"+"[decision]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t"+
							r.getString(2)
					);
				}
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<View test 2>");
				
				String TQuery3 = "create or replace function CSacceptDelete() returns trigger as $$\r\n" + 
						"begin\r\n" + 
						"Delete from Apply\r\n" + 
						"where sID = Old.sID\r\n"+
						"and cName = Old.cName and major = 'CS' and decision = 'Y';"+
						"return New;\r\n"+
						"end;\r\n" +
						"$$\r\n"+
						"language 'plpgsql';\r\n" + 
						"create trigger R3\r\n" + 
						"instead of delete on CSaccept\n" + 
						"for each row\r\n" + 
						"execute procedure CSacceptDelete();"; 
				stm.executeUpdate(TQuery3); //create Trigger3
				System.out.println("Trigger CSacceptDelete is created\n");
				
				String DeleteQuery = "delete from CSaccept where sID = 123";
				stm.executeUpdate(DeleteQuery); //Delete Data
				System.out.println("Data is Deleted.\n");
				
				String Query4 = "(select * from CSaccept)"; //Display
				r = getQuery(Query4);
				System.out.println("<Result>");
				System.out.println("[sID]\t"+"[cName]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t"+
							r.getString(2)
					);
				}
				System.out.println("\n");
				Query4 = "(select * from Apply)";
				r = getQuery(Query4);
				System.out.println("[sID]\t"+"[cName]\t"+"[major]\t"+"[decision]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t"+
							r.getString(2)+"\t"+
							r.getString(3)+"\t"+
							r.getString(4)
					);
				}
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<View test 3>");
				
				String TQuery4 = "create or replace function CSacceptUpdate() returns trigger as $$\r\n" + 
						"begin\r\n" + 
						"Update Apply\r\n" + 
						"set cName = New.cName\r\n"+
						"where sID = Old.sID\r\n"+
						"and cName = Old.cName and major = 'CS' and decision = 'Y';"+
						"return New;\r\n"+
						"end;\r\n" +
						"$$\r\n"+
						"language 'plpgsql';\r\n" + 
						"create trigger CSacceptUpdate\r\n" + 
						"instead of Update on CSaccept\n" + 
						"for each row\r\n" + 
						"execute procedure CSacceptUpdate();"; 
				stm.executeUpdate(TQuery4); //Create Trigger4
				System.out.println("Trigger CSacceptUpdate is Created.\n");
				
				String UpdateQuery = "Update CSaccept set cName = 'CMU' where sID = 345";
				stm.executeUpdate(UpdateQuery); //Update Data
				System.out.println("Table is Updated.\n");
				
				String Query5 = "(select * from CSaccept)"; //Display
				r = getQuery(Query5);
				System.out.println("<Result>");
				System.out.println("[sID]\t"+"[cName]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t"+
							r.getString(2)
					);
				}
				System.out.println("\n");
				Query5 = "(select * from Apply)";
				r = getQuery(Query5);
				System.out.println("[sID]\t"+"[cName]\t"+"[major]\t"+"[decision]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t"+
							r.getString(2)+"\t"+
							r.getString(3)+"\t"+
							r.getString(4)
					);
				}
			}

		} catch (SQLException ex) { throw ex; }
	}

}
