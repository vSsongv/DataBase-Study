/*
 DataBase ASM03 Testing Recursive query, OLAP query
 Song Jinyoung 201721747 Software and Engineering
*/

package DB3;

import java.util.*;
import java.sql.*;

public class SqlTest3 {
	
		static Connection conn;
		static Statement stm;
	    static ResultSet r = null;
	    
	    public static ResultSet getQuery(String Query) throws SQLException
		{
			ResultSet r = stm.executeQuery(Query); // send query
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
			
			System.out.println("<Recursive test 1>");
			
			String RQuery1 = "with recursive\r\n" + 
					"Ancestor(a,d) as (select parent as a, child as d from ParentOf\r\n" + 
					"union\r\n" + 
					"select Ancestor.a, ParentOf.child as d\r\n" + 
					"from Ancestor, ParentOf\r\n" + 
					"where Ancestor.d = ParentOf.parent)\r\n" + 
					"select a from Ancestor where d = 'Mary';"; 

			r = getQuery(RQuery1);
			System.out.println("<Result>");
			System.out.println("[a]"); //Display
			while(r.next())
			{
				System.out.println
				(
					r.getString(1)
				);
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<Recursive test 2>");
				
				String RQuery2 = "with recursive\r\n" + 
						"Route(orig, dest, total) as\r\n" + 
						"(select orig, dest, cost as toal from Flight\r\n" + 
						"union\r\n" + 
						"select R.orig, F.dest, total+cost as total\r\n" + 
						"from Route R, Flight F\r\n" + 
						"where R.dest = F.orig)\r\n" + 
						"select * from Route\r\n" + 
						"where orig = 'A' and dest = 'B';" ;

				r = getQuery(RQuery2);
				System.out.println("<Result>");
				System.out.println("[orig]\t"+"[dest]\t"+"[total]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t"+
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
				System.out.println("<Recursive test 3>");
				
				String RQuery3 = "with recursive\r\n" + 
						"ToB(orig, total) as\r\n" + 
						"(select orig, cost as toal from Flight where dest = 'B'\r\n" + 
						"union\r\n" + 
						"select F.orig, cost+total as total\r\n" + 
						"from Flight F, ToB TB\r\n" + 
						"where F.dest = TB.orig)\r\n" + 
						"select min(total) from Tob where orig = 'A';" ;

				r = getQuery(RQuery3);
				System.out.println("<Result>");
				System.out.println("[min]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)
					);
				}
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<OLAP test 1>");
				
				String OQuery1 = "select storeID, itemID, custID, sum(price)\r\n" + 
						"from Sales\r\n" + 
						"group by cube (storeID, itemID, custID);" ;

				r = getQuery(OQuery1);
				System.out.println("<Result>");
				System.out.println("[storeID] "+"[itemID] "+"[custID] "+"[sum(price)]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t  "+
							r.getString(2)+"\t   "+
							r.getString(3)+"\t  "+
							r.getString(4)
					);
				}
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<OLAP test 2>");
				
				String OQuery2 = "select storeID, itemID, custID, sum(price)\r\n" + 
						"from Sales F\r\n" + 
						"group by itemID, cube (storeID, custID);";

				r = getQuery(OQuery2);
				System.out.println("<Result>");
				System.out.println("[storeID] "+"[itemID] "+"[custID] "+"[sum(price)]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t  "+
							r.getString(2)+"\t   "+
							r.getString(3)+"\t  "+
							r.getString(4)
					);
				}
			}
			
			System.out.println("\nContinue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<OLAP test 3>");
				
				String OQuery3 = "select storeID, itemID, custID, sum(price)\r\n" + 
						"from Sales F\r\n" + 
						"group by rollup(storeID, itemID, custID);";

				r = getQuery(OQuery3);
				System.out.println("<Result>");
				System.out.println("[storeID] "+"[itemID] "+"[custID] "+"[sum(price)]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t  "+
							r.getString(2)+"\t   "+
							r.getString(3)+"\t  "+
							r.getString(4)
					);
				}
			}
		
		} catch (SQLException ex) { throw ex; }


	}

}
