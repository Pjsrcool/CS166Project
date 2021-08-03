/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Date;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 4) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user> <password>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			String password = args[3];
			
			esql = new MechanicShop (dbname, dbport, user, password);
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		Integer id;
		String fname, lname, phone, address;

		try{
			// create new customer id
			Statement S = esql._connection.createStatement();
			ResultSet rs = S.executeQuery("SELECT MAX(id) FROM Customer;");
			rs.next();
			id =  rs.getInt("max") + 1;

			// now we ask for customer information
			System.out.print("Enter customer first name (32 charactes max): ");
			fname = in.readLine();
			if (fname.length() > 32) {
				System.out.println("ERROR: First name must be 32 characters or less!\n");
				return;
			}

			System.out.print("Enter customer last name (32 characters max): " );
			lname = in.readLine();
			if (lname.length() > 32) {
				System.out.println("ERROR: Last name must be 32 characters or less!\n");
				return;
			}

			System.out.print("Enter customer phone number using integers only: ");
			phone = in.readLine();
			if (phone.length() > 13) {
				System.out.println("ERROR: Phone numbers can only be 13 digits or less!\n");
				return;
			}

			System.out.print("Enter customer address (256 characters max): ");
			address = in.readLine();
			if (address.length() > 256) {
				System.out.println("ERROR: Complain description can only be 256 characters or less!\n");
				return;
			}
		
			// execute insertion into table
			esql.executeUpdate("INSERT INTO Customer VALUES (" + id + ", '" + fname + "', '" 
										+ lname + "', '" + phone + "', '" + address  + "')");

			System.out.println("Customer " + fname + " " + lname + " has been added with id " +
								id + ".\n");

		} catch (Exception e) {
			System.out.println("ERROR: Failed to insert customer data. " +
							   "Make sure the customer information is entered correctly.\n");
			System.out.println(e.getMessage());
		}
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		Integer id, experience;
		String fname, lname, temp;

		try{
			// create new customer id
			Statement S = esql._connection.createStatement();
			ResultSet rs = S.executeQuery("SELECT MAX(id) FROM Mechanic;");
			rs.next();
			id =  rs.getInt("max") + 1;

			// now we ask for customer information
			System.out.print("Enter mechanic first name (32 charactes max): ");
			fname = in.readLine();
			if (fname.length() > 32) {
				System.out.println("ERROR: First name must be 32 characters or less!\n");
				return;
			}

			System.out.print("Enter mechanic last name (32 characters max): " );
			lname = in.readLine();
			if (lname.length() > 32) {
				System.out.println("ERROR: Last name must be 32 characters or less!\n");
				return;
			}

			System.out.print("Enter mechanic years of experience using integers only: ");
			temp = in.readLine();
			if (temp.length() > 2) {
				System.out.println("ERROR: max years of experience is 99!\n");
				return;
			}

			experience = Integer.parseInt(temp);

			// execute insertion into table
			esql.executeUpdate("INSERT INTO Mechanic VALUES (" + id + ", '" + fname + "', '" 
										+ lname + "', '" + experience + "')");

			System.out.println("Mechanic " + fname + " " + lname + " has been added with id " +
								id + ".\n");

		} catch (Exception e) {
			System.out.println("ERROR: Failed to insert mechanic data. " +
							   "Make sure the customer information is entered correctly.\n");
			System.out.println(e.getMessage());
		}
	}
	
	public static void AddCar(MechanicShop esql){//3
		
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		Integer rid, customer_id, odometer;
		String car_vin, complain;
		Date date;

		try {
			System.out.print("Enter rid (integer only): ");
			rid = Integer.parseInt(in.readLine());
			System.out.print("Enter customer ID: ");
			customer_id = Integer.parseInt(in.readLine());
			System.out.print("Enter car vin: ");
			car_vin = in.readLine();
			System.out.print("Enter date using numbers in the format year-month-day: ");
			date = Date.valueOf(in.readLine());
			System.out.print("Enter odometer value (integer only): ");
			odometer = Integer.parseInt(in.readLine());
			System.out.print("Enter the complaint: ");
			complain = in.readLine();

			esql.executeUpdate("INSERT INTO Service_Request VALUES ('" + rid + "', '" + customer_id + 
							   "', '" + car_vin + "', '" + date + "', '" + odometer + "', '" + complain + "');");

			System.out.println("New Service Request created sucessfully!\n");
		} catch (NumberFormatException e) {
			System.out.println("ERROR: Please enter an integer");
			System.out.println(e.getMessage() + "\n");
		} catch (IllegalArgumentException e) {
			System.out.println("ERROR: Date is entered incorrectly.\n");
		} catch (Exception e) {
			System.out.println("ERROR: Failed to create Service Request.");
			System.out.println(e.getMessage());
		} 
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		//
		
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
		//
		
	}
	
}