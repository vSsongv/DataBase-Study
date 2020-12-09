/*
 DataBase ASM01 Connection to Server and Operating some Queries
 Song Jinyoung 201721747 Software and Engineering
*/

package DB;

import java.util.*;
import java.sql.*;

public class SqlTest {

	public static void main(String[] args) throws SQLException {
		
		Connection conn;
		Statement stm;
        PreparedStatement pstm = null;
        ResultSet r = null;
        
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
			
			System.out.println("Creating College, Student, Apply relations");
			
			String creation = "create table College(cName text, state text, enrollment integer);\r\n" + 
					"create table Student(sID integer, sName text, GPA float, sizeHS integer);\r\n" + 
					"create table Apply(sID integer, cName text, major text, decision char);\r\n";
			
			stm = conn.createStatement();
			stm.executeUpdate(creation); //create Tables
			System.out.println("Tables are created\n");
			
			System.out.println("Inserting tuples to College, Student, Apply relations");
			
			String InCollege = "insert into College values ('Stanford', 'CA', 15000);" +
						"insert into College values ('Berkeley', 'CA', 36000);" +
						"insert into College values ('MIT', 'MA', 10000);" +
						"insert into College values ('Cornell', 'NY', 21000);";
			
			stm.executeUpdate(InCollege); //Insert Data to College table
			System.out.println("Data are inserted in College Table");
			
			String InStudent = "insert into Student values (123, 'Amy', 3.9, 1000);" +
						"insert into Student values (234, 'Bob', 3.6, 1500);" +
						"insert into Student values (345, 'Craig', 3.5, 500);" +
						"insert into Student values (456, 'Doris', 3.9, 1000);" +
						"insert into Student values (567, 'Edward', 2.9, 2000);" +
						"insert into Student values (678, 'Fay', 3.8, 200);" +
						"insert into Student values (789, 'Gary', 3.4, 800);" +
						"insert into Student values (987, 'Heren', 3.7, 800);" +
						"insert into Student values (876, 'Irene', 3.9, 400);" +
						"insert into Student values (765, 'Jay', 2.9, 1500);" +
						"insert into Student values (654, 'Amy', 3.9, 1000);" +
						"insert into Student values (543, 'Craig', 3.4, 2000);";
			
			stm.executeUpdate(InStudent); //Insert Data to Student table
			System.out.println("Data are inserted in Student Table");
			
			String InApply = "insert into Apply values (123, 'Stanford', 'CS', 'Y'); "+
						"insert into Apply values (123, 'Stanford', 'EE', 'N'); "+
						"insert into Apply values (123, 'Berkeley', 'CS', 'Y'); "+
						"insert into Apply values (123, 'Cornell', 'EE', 'Y'); "+
						"insert into Apply values (234, 'Berkeley', 'biology', 'N'); "+
						"insert into Apply values (345, 'MIT', 'bioengineering', 'Y'); "+
						"insert into Apply values (345, 'Cornell', 'bioengineering', 'N'); "+
						"insert into Apply values (345, 'Cornell', 'CS', 'Y'); "+
						"insert into Apply values (345, 'Cornell', 'EE', 'N'); "+
						"insert into Apply values (678, 'Stanford', 'history', 'Y'); "+
						"insert into Apply values (987, 'Stanford', 'CS', 'Y'); "+
						"insert into Apply values (987, 'Berkeley', 'CS', 'Y'); "+
						"insert into Apply values (876, 'Stanford', 'CS', 'N'); "+
						"insert into Apply values (876, 'MIT', 'biology', 'Y'); "+
						"insert into Apply values (876, 'MIT', 'marinebiology', 'N'); "+
						"insert into Apply values (765, 'Stanford', 'history', 'Y'); "+
						"insert into Apply values (765, 'Cornell', 'history', 'N'); "+
						"insert into Apply values (765, 'Cornell', 'psychology', 'Y'); "+
						"insert into Apply values (543, 'MIT', 'CS', 'N');";
			
			stm.executeUpdate(InApply); //Insert Data to Apply table
			System.out.println("Data are inserted in Apply Table\n");
			
			System.out.println("Continue? (Enter 1 for continue)");
			num = scan.nextInt();
			scan.nextLine();
			
			if(num == 1)	
			{
				System.out.println("<Query 1>");
				String Q1 = "select * from College;"; //select * from College
				pstm = conn.prepareStatement(Q1);
				r = pstm.executeQuery();
				
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
				System.out.println("<Query 2>");
				String Q2 = "select * from Student;"; //select * from Student
				pstm = conn.prepareStatement(Q2);
			    r = pstm.executeQuery();
				
				System.out.println("[sID]\t"+"[sName]\t"+"[GPA]\t"+"[sizeHS]"); //Display
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
				System.out.println("<Query 3>");
				String Q3 = "select * from Apply;"; //select * from Apply
				pstm = conn.prepareStatement(Q3);
				r = pstm.executeQuery();
				
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
				System.out.println("<Query 4>");
				String Q4 = "select * from Student S1 where (select count(*) from Student S2 where S2.sID <> S1.sID and S2.GPA = S1.GPA) =" + 
						"(select count(*) from Student S2 where S2.sID <> S1.sID and S2.sizeHS = S1.sizeHS);"; // Select * from Student who has same GPA
				pstm = conn.prepareStatement(Q4);
				r = pstm.executeQuery();
				
				System.out.println("[sID]\t"+"[sName]\t"+"[GPA]\t"+"[sizeHS]"); //Display
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
				System.out.println("<Query 5>");
				String Q5 = "select Student.sID, sName, count(distinct cName) from Student, Apply\r\n" + 
						"where Student.sID = Apply.sID group by Student.sID, sName;"; // select sID, sName,count who apply College
				pstm = conn.prepareStatement(Q5);
				r = pstm.executeQuery();
				
				System.out.println("[sID]\t"+"[sName]\t"+"[count]\t"); //Display
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
				System.out.println("<Query 6>");
				String Q6 = "select major from Student, Apply where Student.sID = Apply.sID\r\n" + 
						"group by major\r\n having max(GPA) < (select avg(GPA) from Student);"; // select major which is applied by student who has lowest GPA
				pstm = conn.prepareStatement(Q6);
				r = pstm.executeQuery();
				
				System.out.println("[major]\t"); //Display
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
			
			int inSizeHS = 0;
			String inMajor = null;
			String incName = null;			
			
			
			
			if(num == 1)	
			{
				System.out.println("Enter sizeHS : ");
				inSizeHS = scan.nextInt();
				scan.nextLine();
				System.out.println("Enter major : ");
				inMajor = scan.nextLine();
				System.out.println("Enter College Name : ");
				incName = scan.nextLine();
				
				System.out.println("<Query 7>");
				String Q7 = "select sName, GPA from Student join Apply on Student.sID = Apply.sID "
						+ "where sizeHS < ? and major = ? and cName = ?;"; // select sName, GPA student who has sizeHS,major,cName with User's input
				
				pstm = conn.prepareStatement(Q7);
				pstm.setInt(1, inSizeHS);
				pstm.setString(2, inMajor);
				pstm.setString(3, incName);
				
				r = pstm.executeQuery();
				
				System.out.println("[sName]\t"+"[GPA]"); //Display
				while(r.next())
				{
					System.out.println
					(
							r.getString(1)+"\t"+
							r.getString(2)
					);
				}
			}
			
		} catch (SQLException ex) 
		{
			throw ex;
		}
		
		finally {
		     if ( r != null ) try{ r.close(); } catch (Exception e){}
		     if ( pstm != null ) try{ pstm.close(); } catch (Exception e){}
		     if ( conn != null ) try{ conn.close(); } catch (Exception e){}
				}
	}
}