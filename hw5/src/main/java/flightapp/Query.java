package flightapp;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import javax.crypto.*;
import javax.crypto.spec.*;

/**
 * Runs queries against a back-end database
 */
public class Query {
  // DB Connection
  private Connection conn;
  private boolean isLogin;
  private String currentUser;

  // Password hashing parameter constants
  private static final int HASH_STRENGTH = 65536;
  private static final int KEY_LENGTH = 128;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;

  // For clear table
  private static final String CLEARTABLE_USERS_SQL = "delete from USERS";
  private static final String CLEARTABLE_RESERVATIONS_SQL = "delete from RESERVATIONS";
  private PreparedStatement clearUSERStatement;
  private PreparedStatement clearRESERVATIONstatement;

  // For check dangling
  private static final String TRANCOUNT_SQL = "SELECT @@TRANCOUNT AS tran_count";
  private PreparedStatement tranCountStatement;

  // For check user exist for login
  private static final String locateUser_SQL = "SELECT password, salt FROM USERS U WHERE U.username = ?";
  private PreparedStatement locateUserStatement;

  // For search for direct flight
  private static final String directSearch_SQL = "SELECT TOP ( ? ) day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price FROM Flights  WHERE origin_city = ?  AND dest_city =  ? AND day_of_month = ?  ORDER BY actual_time ASC";
  private PreparedStatement directSearchStatement;

  // For search for direct & one stop flight
  private static final String unsafeSearch_SQL = "SELECT TOP ( ? ) day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price FROM Flights  WHERE origin_city = ?  AND dest_city =  ? AND day_of_month = ?  ORDER BY actual_time ASC";
  private PreparedStatement unsafeSearchStatement;

  // TODO: YOUR CODE HERE

  public Query() throws SQLException, IOException {
    this(null, null, null, null);
    isLogin = false;
    currentUser = null;
  }

  protected Query(String serverURL, String dbName, String adminName, String password) throws SQLException, IOException {
    conn = serverURL == null ? openConnectionFromDbConn()
        : openConnectionFromCredential(serverURL, dbName, adminName, password);

    prepareStatements();
  }

  /**
   * Return a connecion by using dbconn.properties file
   *
   * @throws SQLException
   * @throws IOException
   */
  public static Connection openConnectionFromDbConn() throws SQLException, IOException {
    // Connect to the database with the provided connection configuration
    Properties configProps = new Properties();
    configProps.load(new FileInputStream("dbconn.properties"));
    String serverURL = configProps.getProperty("hw5.server_url");
    String dbName = configProps.getProperty("hw5.database_name");
    String adminName = configProps.getProperty("hw5.username");
    String password = configProps.getProperty("hw5.password");
    return openConnectionFromCredential(serverURL, dbName, adminName, password);
  }

  /**
   * Return a connecion by using the provided parameter.
   *
   * @param serverURL example: example.database.widows.net
   * @param dbName    database name
   * @param adminName username to login server
   * @param password  password to login server
   *
   * @throws SQLException
   */
  protected static Connection openConnectionFromCredential(String serverURL, String dbName, String adminName,
      String password) throws SQLException {
    String connectionUrl = String.format("jdbc:sqlserver://%s:1433;databaseName=%s;user=%s;password=%s", serverURL,
        dbName, adminName, password);
    Connection conn = DriverManager.getConnection(connectionUrl);

    // By default, automatically commit after each statement
    conn.setAutoCommit(true);

    // By default, set the transaction isolation level to serializable
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    return conn;
  }

  /**
   * Get underlying connection
   */
  public Connection getConnection() {
    return conn;
  }

  /**
   * Closes the application-to-database connection
   */
  public void closeConnection() throws SQLException {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      clearUSERStatement.executeUpdate();
      clearRESERVATIONstatement.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    tranCountStatement = conn.prepareStatement(TRANCOUNT_SQL);
    clearUSERStatement = conn.prepareStatement(CLEARTABLE_USERS_SQL);
    clearRESERVATIONstatement = conn.prepareStatement(CLEARTABLE_RESERVATIONS_SQL);
    locateUserStatement = conn.prepareStatement(locateUser_SQL);
    directSearchStatement = conn.prepareStatement(directSearch_SQL);

    // TODO: YOUR CODE HERE
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username user's username
   * @param password user's password
   *
   * @return If someone has already logged in, then return "User already logged
   *         in\n" For all other errors, return "Login failed\n". Otherwise,
   *         return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password) {
    try {
      if (isLogin) {
        return "User already logged in\n";
      }
      try {
        locateUserStatement.setString(1, username);
        ResultSet locate_result = locateUserStatement.executeQuery();
        byte[] hash1 = null;
        byte[] hash2 = null;
        byte[] salt = null;

        while (locate_result.next()) {
          salt = locate_result.getBytes("salt");
          // calculate hash based on user input
          KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);

          // Generate the hash
          SecretKeyFactory factory = null;

          try {
            factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            hash1 = factory.generateSecret(spec).getEncoded();
          } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
            throw new IllegalStateException();
          }

          // compare with database hash
          hash2 = locate_result.getBytes("password");

          if (Arrays.equals(hash1, hash2)) {
            locate_result.close();
            currentUser = username;
            isLogin = true;
            return "Logged in as " + username + "\n";
          }
        }
        locate_result.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      return "Login failed\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implement the create user function.
   *
   * @param username   new user's username. User names are unique the system.
   * @param password   new user's password.
   * @param initAmount initial amount to deposit into the user's account, should
   *                   be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n"
   *         if failed.
   */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    try {
      try {

        // check balance
        if (initAmount < 0) {
          return "Failed to create user\n";
        }

        // check username
        String isUsernameExist = "SELECT * FROM USERS U WHERE U.username = \'" + username + "\'";
        Statement checkUsername = conn.createStatement();
        ResultSet checkUsernameResult = checkUsername.executeQuery(isUsernameExist);

        while (checkUsernameResult.next()) {
          return "Failed to create user\n";
        }
        checkUsernameResult.close();

        // generate password
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        // Specify the hash parameters
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);
        // Generate the hash
        SecretKeyFactory factory = null;
        byte[] hash = null;
        try {
          factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          hash = factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
          throw new IllegalStateException();
        }

        String insertSQL = "INSERT INTO USERS (username, password, salt, balance) " + "VALUES (\'" + username
            + "\', ? , ? , " + initAmount + ")";

        PreparedStatement insertStatement = conn.prepareStatement(insertSQL);
        insertStatement.setBytes(1, hash);
        insertStatement.setBytes(2, salt);
        insertStatement.executeUpdate();
        insertStatement.close();
        return "Created user " + username + "\n";

      } catch (SQLException e) {
        e.printStackTrace();
      }
      return "Failed to create user\n";
    } finally {
      checkDanglingTransaction();

    }
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights and
   * flights with two "hops." Only searches for up to the number of itineraries
   * given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight        if true, then only search for direct flights,
   *                            otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your
   *         selection\n". If an error occurs, then return "Failed to search\n".
   *
   *         Otherwise, the sorted itineraries printed in the following format:
   *
   *         Itinerary [itinerary number]: [number of flights] flight(s), [total
   *         flight time] minutes\n [first flight in itinerary]\n ... [last flight
   *         in itinerary]\n
   *
   *         Each flight should be printed using the same format as in the
   *         {@code Flight} class. Itinerary numbers in each search should always
   *         start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth, int numberOfItineraries) {
    try {
      // WARNING the below code is unsafe and only handles searches for direct flights
      // You can use the below code as a starting reference point or you can get rid
      // of it all and replace it with your own implementation.
      //
      if (directFlight) {
        // direct flight 

        StringBuffer sb = new StringBuffer();
        try {
          unsafeSearchStatement.setInt(1, numberOfItineraries);
          unsafeSearchStatement.setString(2, originCity);
          unsafeSearchStatement.setString(3, destinationCity);
          unsafeSearchStatement.setInt(4, dayOfMonth);
          unsafeSearchStatement.executeUpdate();
          unsafeSearchStatement.close();
          ResultSet oneHopResults = unsafeSearchStatement.executeQuery();

          while (oneHopResults.next()) {
            int result_dayOfMonth = oneHopResults.getInt("day_of_month");
            String result_carrierId = oneHopResults.getString("carrier_id");
            String result_flightNum = oneHopResults.getString("flight_num");
            String result_originCity = oneHopResults.getString("origin_city");
            String result_destCity = oneHopResults.getString("dest_city");
            int result_time = oneHopResults.getInt("actual_time");
            int result_capacity = oneHopResults.getInt("capacity");
            int result_price = oneHopResults.getInt("price");

            sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum
                + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time
                + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
          } 
          oneHopResults.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
        if (sb.length() == 0){
          return "No flights match your selection\n";
        } else {
        return sb.toString();
        }
      } 
      // one stop flight
      else {
        StringBuffer sb = new StringBuffer();
        

        try {
          // one hop itineraries
          directSearchStatement.setInt(1, numberOfItineraries);
          directSearchStatement.setString(2, originCity);
          directSearchStatement.setString(3, destinationCity);
          directSearchStatement.setInt(4, dayOfMonth);
          directSearchStatement.executeUpdate();
          directSearchStatement.close();
          ResultSet oneHopResults = directSearchStatement.executeQuery();

          while (oneHopResults.next()) {
            int result_dayOfMonth = oneHopResults.getInt("day_of_month");
            String result_carrierId = oneHopResults.getString("carrier_id");
            String result_flightNum = oneHopResults.getString("flight_num");
            String result_originCity = oneHopResults.getString("origin_city");
            String result_destCity = oneHopResults.getString("dest_city");
            int result_time = oneHopResults.getInt("actual_time");
            int result_capacity = oneHopResults.getInt("capacity");
            int result_price = oneHopResults.getInt("price");

            sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum
                + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time
                + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
          } 
          oneHopResults.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
        if (sb.length() == 0){
          return "No flights match your selection\n";
        } else {
        return sb.toString();
        } 
      }
    } finally {
      checkDanglingTransaction();
    }

  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is
   *                    returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations,
   *         not logged in\n". If the user is trying to book an itinerary with an
   *         invalid ID or without having done a search, then return "No such
   *         itinerary {@code itineraryId}\n". If the user already has a
   *         reservation on the same day as the one that they are trying to book
   *         now, then return "You cannot book two flights in the same day\n". For
   *         all other errors, return "Booking failed\n".
   *
   *         And if booking succeeded, return "Booked flight(s), reservation ID:
   *         [reservationId]\n" where reservationId is a unique number in the
   *         reservation system that starts from 1 and increments by 1 each time a
   *         successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId) {
    try {
      // TODO: YOUR CODE HERE
      return "Booking failed\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   *         If the reservation is not found / not under the logged in user's
   *         name, then return "Cannot find unpaid reservation [reservationId]
   *         under user: [username]\n" If the user does not have enough money in
   *         their account, then return "User has only [balance] in account but
   *         itinerary costs [cost]\n" For all other errors, return "Failed to pay
   *         for reservation [reservationId]\n"
   *
   *         If successful, return "Paid reservation: [reservationId] remaining
   *         balance: [balance]\n" where [balance] is the remaining balance in the
   *         user's account.
   */
  public String transaction_pay(int reservationId) {
    try {
      // TODO: YOUR CODE HERE
      return "Failed to pay for reservation " + reservationId + "\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not
   *         logged in\n" If the user has no reservations, then return "No
   *         reservations found\n" For all other errors, return "Failed to
   *         retrieve reservations\n"
   *
   *         Otherwise return the reservations in the following format:
   *
   *         Reservation [reservation ID] paid: [true or false]:\n [flight 1 under
   *         the reservation]\n [flight 2 under the reservation]\n Reservation
   *         [reservation ID] paid: [true or false]:\n [flight 1 under the
   *         reservation]\n [flight 2 under the reservation]\n ...
   *
   *         Each flight should be printed using the same format as in the
   *         {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations() {
    try {
      // TODO: YOUR CODE HERE
      return "Failed to retrieve reservations\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations,
   *         not logged in\n" For all other errors, return "Failed to cancel
   *         reservation [reservationId]\n"
   *
   *         If successful, return "Canceled reservation [reservationId]\n"
   *
   *         Even though a reservation has been canceled, its ID should not be
   *         reused by the system.
   */
  public String transaction_cancel(int reservationId) {
    try {
      // TODO: YOUR CODE HERE
      return "Failed to cancel reservation " + reservationId + "\n";
    } finally {
      checkDanglingTransaction();
    }
  }

  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  /**
   * Throw IllegalStateException if transaction not completely complete, rollback.
   * 
   */
  private void checkDanglingTransaction() {
    try {
      try (ResultSet rs = tranCountStatement.executeQuery()) {
        rs.next();
        int count = rs.getInt("tran_count");
        if (count > 0) {
          throw new IllegalStateException(
              "Transaction not fully commit/rollback. Number of transaction in process: " + count);
        }
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Database error", e);
    }
  }

  private static boolean isDeadLock(SQLException ex) {
    return ex.getErrorCode() == 1205;
  }

  /**
   * A class to store flight information.
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: " + flightNum + " Origin: "
          + originCity + " Dest: " + destCity + " Duration: " + time + " Capacity: " + capacity + " Price: " + price;
    }
  }
}
