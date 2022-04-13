
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class moviedb {
	
	
	//for deleting the DB movies table
	private static void deleteTable(Connection conn) throws SQLException {
		String deleteTableQuery ="DROP TABLE Movies";
		Statement deleteTableStmt = conn.createStatement();
		deleteTableStmt.execute(deleteTableQuery);		
	}
	
	// Method to insert rows to the database
		private static void insertMovie(Connection conn, String name, String actor, String actress, String director, int year) throws SQLException {
				
				String insertQuery ="INSERT INTO Movies(Name,Actor,Actress,Director,Year) VALUES(?,?,?,?,?)";
				PreparedStatement insertStmt = conn.prepareStatement(insertQuery);
				insertStmt.setString(1, name);
				insertStmt.setString(2, actor);
				insertStmt.setString(3, actress);
				insertStmt.setString(4, director);
				insertStmt.setInt(5, year);
				insertStmt.executeUpdate();	
		}
		
		
		// Method to display all entries from database table
		private static void displayDB(Connection conn, String tableName) throws SQLException {
			
				String selectQuery ="SELECT * from " + tableName;
				Statement stmt = conn.createStatement();
				ResultSet selectResult = stmt.executeQuery(selectQuery);
				
				while(selectResult.next()) {
					
					String name = selectResult.getString("Name");
					String actor = selectResult.getString("Actor");
					String actress = selectResult.getString("Actress");
					String director = selectResult.getString("Director");
					String year = selectResult.getString("Year");
					
					System.out.println(name+"\t"+actor+"\t"+actress+"\t"+director+"\t"+year);
				}					
		}
		
		// Method to display movies by filtering according to actor name
		private static void displayFilterActor(Connection conn, String tableName, String actor) throws SQLException {
			String selectSQL ="SELECT Name from " + tableName + " WHERE Actor = '" + actor +"'";
			Statement stmt=conn.createStatement();
			ResultSet rs=stmt.executeQuery(selectSQL);
			System.out.println("Movies by actor " + actor);
			while(rs.next()) {
				System.out.println(rs.getString("Name"));
			}
		}
		
		// Method to display movies by filtering according to director name
				private static void displayFilterDirector(Connection conn, String tableName, String director) throws SQLException {
					String selectSQL ="SELECT Name from " + tableName + " WHERE Director = '" + director +"'";
					Statement stmt=conn.createStatement();
					ResultSet rs=stmt.executeQuery(selectSQL);
					System.out.println("Movies by director " + director);
					while(rs.next()) {
						System.out.println(rs.getString("Name"));
					}
				}
				
				// Method to display movies by filtering according to director name
				private static void displayFilterYear(Connection conn, String tableName, String year) throws SQLException {
					String selectSQL ="SELECT Name from " + tableName + " WHERE Year = '" + year +"'";
					Statement stmt=conn.createStatement();
					ResultSet rs=stmt.executeQuery(selectSQL);
					System.out.println("Movies by Release Date "+year);
					while(rs.next()) {
						System.out.println(rs.getString("Name"));
					}
				}

			// Main method
			public static void main(String[] args) {
				
				Connection conn  = null;
				
				try {
					conn=DriverManager.getConnection("jdbc:sqlite:C:/moviesdb/db/moviedb.db");
					System.out.println("Database connection successful\n");
					try {
						deleteTable(conn);
					}
					catch(Exception e)
					{
						System.out.println("Exception occurred : " + e);
					}
					
					// Creating 'Movies' table
					String createTablesql ="CREATE TABLE IF NOT EXISTS movies (Name text, Actor text NOT NULL, Actress text NOT NULL, Director text NOT NULL, Year integer);";  
					Statement stmt=conn.createStatement();
					stmt.execute(createTablesql);
					
					// Inserting values into 'Movies' table			
					insertMovie (conn,"James","Puneeth Rajakumar","Priya","Santosh Anandaram",2022);
					insertMovie (conn,"KGF2","Yash","Srinidhi Shetty","Prashant neel",2022);
					insertMovie (conn,"Shershaah","Sidharth Malhotra","Kiara Advani","Vishnuvardhan",2021);
					insertMovie (conn,"RRR","Ram charan and NTR","Alia Batt","Rajmouli",2022);
					insertMovie (conn,"Marshel","Vijay","Samanta","Atlee",2017);
					insertMovie (conn,"Zero","Shah Rukh Khan","Anushka","Anand L.Rai",2018);
					insertMovie (conn,"Sooryavanshi","Akshay Kumar","Niharica","Rohith Shetty",2021);
					insertMovie (conn,"Yuvaratna","Puneeth Rajakumar","Sayyeshaa","Santosh Anandaram",2021);

					// Displaying entire table
					displayDB(conn,"Movies");
					
					System.out.println();
					// Filtering movie according to actor
					displayFilterActor(conn,"Movies","Puneeth Rajakumar");
					
					System.out.println();
					// Filtering movie according to release date
					displayFilterYear(conn,"Movies","2021");
					
					
				} catch (SQLException e) { 
					
					System.out.println("Exception occurred : " + e);
					
				} finally {
					
					if(conn!=null) {
						try {
							conn.close();
						}
						catch(SQLException e) {
							e.printStackTrace();
							System.out.println(e.getMessage());
						}
					}
				}
			}
		
		
}
