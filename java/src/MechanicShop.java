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
				System.out.println("Returning to main menu...\n");
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

			System.out.println("\nCustomer " + fname + " " + lname + " has been added with id " + id + ".\n");
			esql.executeQueryAndPrintResult("SELECT * FROM Customer WHERE id = " + id + ";");

		} catch (Exception e) {
			System.out.println("ERROR: Failed to insert customer data. " +
							   "Make sure the customer information is entered correctly.\n");
			System.out.println(e.getMessage());
		} finally {
			System.out.println();
		}
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		Integer id, experience;
		String fname, lname, temp;

		try{
			// create new mechanic id
			Statement S = esql._connection.createStatement();
			ResultSet rs = S.executeQuery("SELECT MAX(id) FROM Mechanic;");
			rs.next();
			id =  rs.getInt("max") + 1;

			// now we ask for mechanic information
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

			System.out.println("\nMechanic " + fname + " " + lname + " has been added with id " + id + ".\n");
			esql.executeQueryAndPrintResult("SELECT * FROM Mechanic WHERE id = " + id + ";");

		} catch (Exception e) {
			System.out.println("ERROR: Failed to insert mechanic data. " +
							   "Make sure the mechanic information is entered correctly.\n");
			System.out.println(e.getMessage());
		} finally {
			System.out.println();
		}
	}
	
	// changed return type void --> String to make InsertServiceRequest significantly easier
	public static String AddCar(MechanicShop esql){//3
		String vin = "", make, model, cLname;
		Integer year, customer_id, oid;

		try {
			// ask for customer name
			Statement S = esql._connection.createStatement();
			ResultSet rs = S.executeQuery("SELECT MAX(rid) FROM Service_Request;");
			rs.next();
			System.out.print("\nEnter customer Last Name: ");
			cLname = in.readLine();

			// using customer name, we query/add new customer
			List<List<String>> customers = esql.executeQueryAndReturnResult(
				"SELECT id, fname, lname FROM Customer WHERE lname = '" + cLname + "';");
			if (customers.size() == 1) {	// case where only 1 customer was found
				System.out.print("Is " + customers.get(0).get(1) + " " + customers.get(0).get(2) + " correct? (y/n): ");
				String answer = in.readLine();
				if (answer.equals("y") || answer.equals("Y")) { // the 1 customer is the correct one
					customer_id = Integer.parseInt(customers.get(0).get(0));
					System.out.println("Customer " + customers.get(0).get(1) + " " + customers.get(0).get(2) + 
									   " with id " + customers.get(0).get(0) + " sucessfully selected.");	
				} else {	// the 1 customer is the wrong one. we add a new customer
					System.out.println("Customer does not exist. Fill out customer form below.\n");
					AddCustomer(esql);
					rs = S.executeQuery("SELECT MAX(id) FROM Customer;");
					rs.next();
					customer_id = rs.getInt("max");
					rs = S.executeQuery("SELECT fname, lname FROM Customer WHERE id = '" + customer_id + "';");
					rs.next();
					System.out.println("Customer " + rs.getString("fname") + " " + rs.getString("lname") +
									   " with id " + customer_id + " sucessfully selected.");
				}
			} else if (customers.size() == 0) {		// case where no customers were found. Immediately add customer
				System.out.println("Customer does not exist. FIll out customer form below.\n");

				AddCustomer(esql);
				rs.close();
				rs = S.executeQuery("SELECT MAX(id) FROM Customer;");
				rs.next();
				customer_id = rs.getInt("max");
				rs = S.executeQuery("SELECT fname, lname FROM Customer WHERE id = '" + customer_id + "';");
				rs.next();
				System.out.println("Customer " + rs.getString("fname") + " " + rs.getString("lname") +
									" with id " + customer_id + " sucessfully selected.");			
			} else {	// case where more than 1 customers are found
				// list out the customers
				esql.executeQueryAndPrintResult("SELECT id, fname, lname FROM Customer WHERE lname = '" + cLname + "';");

				// user selectst the customer
				System.out.print("Enter the customer id from the list above (enter 'x' if not found): ");
				String answer = in.readLine();

				if (!answer.equals("x") && !answer.equals("X")) {	// customer is found
					customer_id = Integer.parseInt(answer);
					List<List<String>> customers2 = esql.executeQueryAndReturnResult(
						"SELECT id, fname, lname FROM Customer WHERE (id = '" + customer_id + "' AND lname = '" + cLname + "');");
					System.out.println("Customer " + customers2.get(0).get(1) + " " + customers2.get(0).get(2) + " with id " +
									   customers2.get(0).get(0) + " sucessfully selected.");
				} else {	// customer is not found
					AddCustomer(esql);
					rs.close();
					rs = S.executeQuery("SELECT MAX(id) FROM Customer;");
					rs.next();
					customer_id = rs.getInt("max") + 1;
					rs = S.executeQuery("SELECT fname, lname FROM Customer WHERE id = '" + customer_id + "';");
					rs.next();
					System.out.println("Customer " + rs.getString("fname") + " " + rs.getString("lname") +
									   " with id " + customer_id + " sucessfully selected.");
				}
			} // finished selecting customer

			// input vin of car
			System.out.print("Enter the car's VIN (6 letters followed by 10 integers): ");
			vin = in.readLine();
			if (vin.length() != 16) {
				throw new Exception("ERROR: Too many or missing characters or numbers!");
			}

			// input make of car
			System.out.print("Enter make of the car (32 charactes max): ");
			make = in.readLine();
			if (make.length() > 32) {
				throw new Exception("ERROR: Too many characters!");
			}

			// input model of car
			System.out.print("Enter model of the car (32 characters max): ");
			model = in.readLine();
			if (model.length() > 32) {
				throw new Exception("ERROR: Too many characters!");
			}

			// input year of car
			System.out.print("Enter year of the car (>= 1970): ");
			year = Integer.parseInt(in.readLine());
			if (year < 1970) {
				throw new Exception("ERROR: Invalid year!");
			}

			// insert car into the Car table, then output success msg to the console
			esql.executeUpdate("INSERT INTO Car VALUES ('" + vin + "','" + make + "','" + model + "','" + year + "');");
			System.out.println("\nSucessfully added new " + make + " " + model + "\n");

			// assigns owner to the car we just added and insert it into the Owns table
			rs = S.executeQuery("SELECT MAX(ownership_id) FROM Owns;");
			rs.next();
			oid = rs.getInt("max") + 1;
			esql.executeUpdate("INSERT INTO Owns VALUES ('" + oid + "','" + customer_id + "','" + vin + "');");
			esql.executeQueryAndPrintResult("SELECT * FROM Owns WHERE ownership_id = " + oid + ";");
			System.out.println();

			esql.executeQueryAndPrintResult("SELECT * FROM Car WHERE vin = '" + vin + "';");

		} catch (Exception e) {
			System.out.println("ERROR: Failed to add new car.");
			System.out.println(e.getMessage() + "\n");
		} finally {
			System.out.println();
		}

		return vin;
	}

	/** Overload of the AddCar(esql) function
	*   This handles the case in InsertServiceRequest where customer already exists in the database
	*	@param Mechanic shop (itself basically), and customer id (owner of the car that is to be added)
	*	@return vin of the car as a String
	*/ 
	public static String AddCar(MechanicShop esql, Integer customer_id){
		String vin = "", make, model;
		Integer year, oid;

		try {
			// input vin 
			System.out.print("Enter the car's VIN (6 letters followed by 10 integers): ");
			vin = in.readLine();
			if (vin.length() != 16) {
				throw new Exception("ERROR: Too many or missing characters or numbers!");
			}

			// input make of car
			System.out.print("Enter make of the car (32 charactes max): ");
			make = in.readLine();
			if (make.length() > 32) {
				throw new Exception("ERROR: Too many characters!");
			}

			// input model of car
			System.out.print("Enter model of the car (32 characters max): ");
			model = in.readLine();
			if (model.length() > 32) {
				throw new Exception("ERROR: Too many characters!");
			}

			// input model year of car
			System.out.print("Enter year of the car (>= 1970): ");
			year = Integer.parseInt(in.readLine());
			if (year < 1970) {
				throw new Exception("ERROR: Invalid year!");
			}

			// insert into the Car table and output a success msg to the console
			esql.executeUpdate("INSERT INTO Car VALUES ('" + vin + "','" + make + "','" + model + "','" + year + "');");
			System.out.println("\nSucessfully added new " + make + " " + model + "\n");

			// assigns owner to  the car we just added and insert it into the Owns table
			Statement S = esql._connection.createStatement();
			ResultSet rs = S.executeQuery("SELECT MAX(ownership_id) FROM Owns;");
			rs.next();
			oid = rs.getInt("max") + 1;
			esql.executeUpdate("INSERT INTO Owns VALUES ('" + oid + "','" + customer_id + "','" + vin + "');");
			esql.executeQueryAndPrintResult("SELECT * FROM Owns WHERE ownership_id = " + oid + ";");
			System.out.println();

			esql.executeQueryAndPrintResult("SELECT * FROM Car WHERE vin = '" + vin + "';");

		} catch (Exception e) {
			System.out.println("ERROR: Failed to add new car.");
			System.out.println(e.getMessage() + "\n");
		} finally {
			System.out.println();
		}

		return vin;
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		Integer rid, customer_id, odometer;
		String cLname, car_vin, complain;
		Date date;
		Boolean customerFound = false;

		try {
			// create a unique rid
			Statement S = esql._connection.createStatement();
			ResultSet rs = S.executeQuery("SELECT MAX(rid) FROM Service_Request;");
			rs.next();
			rid = rs.getInt("max") + 1;

			// ask for customer name
			System.out.print("\nEnter customer Last Name: ");
			cLname = in.readLine();

			// using customer name, we query/add new customer
			List<List<String>> customers = esql.executeQueryAndReturnResult(
				"SELECT id, fname, lname FROM Customer WHERE lname = '" + cLname + "';");
			if (customers.size() == 1) {	// case where only 1 customer was found
				System.out.print("Is " + customers.get(0).get(1) + " " + customers.get(0).get(2) + " correct? (y/n): ");
				String answer = in.readLine();
				if (answer.equals("y") || answer.equals("Y")) { // the 1 customer is the correct one
					customer_id = Integer.parseInt(customers.get(0).get(0));
					System.out.println("Customer " + customers.get(0).get(1) + " " + customers.get(0).get(2) + 
									   " with id " + customers.get(0).get(0) + " sucessfully selected.");
					
					customerFound = true;
				} else {	// the 1 customer is the wrong one. we add a new customer
					System.out.println("Customer does not exist. Fill out customer form below.\n");
					AddCustomer(esql);
					rs = S.executeQuery("SELECT MAX(id) FROM Customer;");
					rs.next();
					customer_id = rs.getInt("max");
					rs = S.executeQuery("SELECT fname, lname FROM Customer WHERE id = '" + customer_id + "';");
					rs.next();
					System.out.println("Customer " + rs.getString("fname") + " " + rs.getString("lname") +
									   " with id " + customer_id + " sucessfully selected.");
				}
			} else if (customers.size() == 0) {		// case where no customers were found. Immediately add customer
				System.out.println("Customer does not exist. FIll out customer form below.\n");

				AddCustomer(esql);
				rs.close();
				rs = S.executeQuery("SELECT MAX(id) FROM Customer;");
				rs.next();
				customer_id = rs.getInt("max");
				rs = S.executeQuery("SELECT fname, lname FROM Customer WHERE id = '" + customer_id + "';");
				rs.next();
				System.out.println("Customer " + rs.getString("fname") + " " + rs.getString("lname") +
									" with id " + customer_id + " sucessfully selected.");			
			} else {	// case where more than 1 customers are found
				// list out the customers
				esql.executeQueryAndPrintResult("SELECT id, fname, lname FROM Customer WHERE lname = '" + cLname + "';");

				// user selectst the customer
				System.out.print("Enter the customer id from the list above (enter 'x' if not found): ");
				String answer = in.readLine();

				if (!answer.equals("x") && !answer.equals("X")) {	// customer is found
					customer_id = Integer.parseInt(answer);
					List<List<String>> customers2 = esql.executeQueryAndReturnResult(
						"SELECT id, fname, lname FROM Customer WHERE (id = '" + customer_id + "' AND lname = '" + cLname + "');");
					System.out.println("Customer " + customers2.get(0).get(1) + " " + customers2.get(0).get(2) + " with id " +
									   customers2.get(0).get(0) + " sucessfully selected.");
				
					customerFound = true;
				} else {	// customer is not found
					AddCustomer(esql);
					rs.close();
					rs = S.executeQuery("SELECT MAX(id) FROM Customer;");
					rs.next();
					customer_id = rs.getInt("max") + 1;
					rs = S.executeQuery("SELECT fname, lname FROM Customer WHERE id = '" + customer_id + "';");
					rs.next();
					System.out.println("Customer " + rs.getString("fname") + " " + rs.getString("lname") +
									   " with id " + customer_id + " sucessfully selected.");
				}
			} // finished selecting customer
					
			// select car
			System.out.println();
			Integer numResults = esql.executeQueryAndPrintResult(
				"SELECT car_vin FROM Owns WHERE customer_id = '" + customer_id + "';"
			);
			if (customerFound && numResults > 0) {	// case where the customer exists in the database and owns cars
				System.out.print("Enter your car's vin from the list above (x if not listed) :");
				car_vin = in.readLine();

				if (car_vin.equals("x") || car_vin.equals("X")) {	// case where customer's car does not exist
					car_vin = esql.AddCar(esql, customer_id);
				}	
			} else {	// case where customer was not in the database or the customer exists but owns no cars
				System.out.println("This customer owns no cars. Let's add one\n");
				car_vin = esql.AddCar(esql, customer_id);
			} 
			
			// ensure that car's vin is correctly entered
			if (car_vin.length() != 16)
						throw new Exception("ERROR: Make sure the vin is entered correctly.");
			// finish selecting car
			
			// select current date
			date = new Date(System.currentTimeMillis());

			// input odometer value
			System.out.print("Enter odometer value (integer only): ");
			odometer = Integer.parseInt(in.readLine());

			// input complaint
			System.out.print("Enter the complaint: ");
			complain = in.readLine();

			// insert service request into database, then we output a sucess msg to the console
			esql.executeUpdate("INSERT INTO Service_Request VALUES ('" + rid + "', '" + customer_id + 
							   "', '" + car_vin + "', '" + date + "', '" + odometer + "', '" + complain + "');");

			System.out.println("\nNew Service Request created sucessfully!\n");
			esql.executeQueryAndPrintResult("SELECT * FROM Service_Request WHERE rid = " + rid + ";");
			
		} catch (NumberFormatException e) {
			System.out.println("ERROR: Please enter an integer");
			System.out.println(e.getMessage() + "\n");
		} catch (Exception e) {
			System.out.println("ERROR: Failed to create Service Request.");
			System.out.println(e.getMessage() + "\n");
		} finally {
			System.out.println();
		}
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		Integer wid, rid, mid, bill;
		Date date;	// closing date
		String comment;

		Boolean found = false;	// control input loops

		try {
			Statement s = esql._connection.createStatement();
			ResultSet rs = s.executeQuery("SELECT MAX(wid) FROM Closed_Request;");
			rs.next();
			wid = rs.getInt("max") + 1;	// wid is 1 bigger than current biggest wid

			//  Select rid from list of unclosed service requests
			esql.executeQueryAndPrintResult(
				"SELECT * FROM Service_Request WHERE rid NOT IN (" +
					"SELECT rid FROM Closed_Request);"
			);
			System.out.print("Enter rid of the service request from list above: ");
			rid = Integer.parseInt(in.readLine());
			while (!found) {
				List<List<String>> results = esql.executeQueryAndReturnResult(
					"SELECT rid " +
					"FROM Service_Request " +
					"WHERE rid = " + rid + " and NOT EXISTS (" +
						"SELECT rid " +
						"FROM Closed_Request " +
						"WHERE rid = " + rid + ");"
				);
				if (results.size() != 1) {
					System.out.print("Service Request is already closed or does not exist. Try another one: ");
					rid = Integer.parseInt(in.readLine());
				} else {
					List<List<String>> car = esql.executeQueryAndReturnResult("SELECT C.make, C.model, S.complain FROM Car C, Service_Request S WHERE C.vin = S.car_vin and rid = " + rid + ";");
					System.out.print("Is the vehicle '" + car.get(0).get(0) + " " + car.get(0).get(1) + "' with issue '" + car.get(0).get(2) + "' correct? (Y/N): ");
					String answer = in.readLine();
					if (answer.equals("Y") || answer.equals("y")) {
						found = true;
						System.out.println("Service Request selected successfully.\n");
					} else if (answer.equals("N") || answer.equals("n")) {
						System.out.print("Enter another rid: ");
						rid = Integer.parseInt(in.readLine());
					}
				}
			} // end input rid

			// input employee
			found = false;
			System.out.print("Enter mechanic ID: ");
			mid = Integer.parseInt(in.readLine());
			while (!found) {
				List<List<String>> result = esql.executeQueryAndReturnResult(
					"SELECT * FROM Mechanic WHERE id = " + mid + ";"
				);

				if (result.size() != 1) {
					System.out.print("ERROR: Invalid ID. Try again: ");
					mid = Integer.parseInt(in.readLine());
				} else {
					System.out.print("Is '" + result.get(0).get(1) + " " + result.get(0).get(2) + "' correct? (Y/N): ");
					String answer = in.readLine();
					if (answer.equals("Y") || answer.equals("y")) {
						found = true;
						System.out.println("Mechanic selected successfully.\n");
					} else if (answer.equals("N") || answer.equals("n")) {
						System.out.print("Enter another ID: ");
						mid = Integer.parseInt(in.readLine());
					}
				}
			} // end input employee

			// select current date
			date = new Date(System.currentTimeMillis());

			// input comment
			System.out.print("Enter any comments: ");
			comment = in.readLine();

			// input bill
			System.out.print("Enter bill amount rounded to the nearest dollar: ");
			bill = Integer.parseInt(in.readLine());

			esql.executeUpdate(
				"INSERT INTO Closed_Request VALUES (" + wid + ", " + rid + ", " + mid + ", '" +
				date + "', '" + comment + "', " + bill + ");"
			);
			System.out.println("\nService Request closed successfully!");
			esql.executeQueryAndPrintResult("SELECT * FROM Closed_Request WHERE wid = " + wid + ";");

		} catch (NumberFormatException e) {
			System.out.println("ERROR: Letters were entered where only numbers are allowed.");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			System.out.println();
		}
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try {
			String query = "SELECT R.date, R.comment, R.bill FROM Closed_Request R WHERE R.bill < 100";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println();
		}
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try {
			String query = "SELECT C.fname, C.lname, COUNT(O.car_vin) AS number_of_cars FROM Customer C, Owns O WHERE O.customer_id = C.id GROUP BY C.id HAVING COUNT(O.car_vin) > 20";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println();
		}
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try {
			String query = "SELECT C.make, C.model, C.year FROM Car C, Service_Request S WHERE C.vin = S.car_vin AND S.odometer < 50000 AND C.year < 1995";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println("");
		}
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		try {
			String query = "SELECT C.vin, C.make, C.model, COUNT(S.rid) AS numberOfRequests FROM Car C, Service_Request S WHERE C.vin = S.car_vin AND S.rid NOT IN (SELECT R.rid FROM Closed_Request R) GROUP BY C.vin HAVING COUNT(S.rid) < ";
			System.out.print("\tEnter max amount of service Requests (k>0): ");
			String k = in.readLine();
			query += k;
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println("");
		}
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
		try {
			String query = "SELECT C.id, C.fname, C.lname, SUM(R.bill) AS totalBill FROM Customer C, Closed_Request R, Service_Request S WHERE C.id = S.customer_id AND R.rid = S.rid GROUP BY C.id ORDER BY totalBill DESC";
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.out.println("");
		}
	}
	
}