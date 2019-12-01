package ceng.ceng351.bookdb;

import com.mysql.cj.protocol.Resultset;

import java.sql.*;
import java.util.Vector;


public class BOOKDB implements IBOOKDB {

    private static String user = "2310233"; // TODO: Your userName
    private static String password = "b96b7429"; //  TODO: Your password
    private static String host = "144.122.71.65"; // host name
    private static String database = "db2310233"; // TODO: Your database name
    private static int port = 8084; // port
    private static Connection con = null;

    public void initialize()
    {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(url, user, password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public int createTables(){
        int table_number = 0;
        int result;
        Vector<String> create_table = new Vector<>();
        String sql1 = "CREATE TABLE IF NOT EXISTS author(author_id INTEGER, author_name VARCHAR(60), PRIMARY KEY(author_id));";
        String sql2 = "CREATE TABLE IF NOT EXISTS publisher(publisher_id INTEGER, publisher_name VARCHAR(50), PRIMARY KEY(publisher_id));";
        String sql3 = "CREATE TABLE IF NOT EXISTS book(isbn CHAR(13), book_name VARCHAR(120), publisher_id INTEGER, first_publish_year CHAR(4), " +
                      "page_count INTEGER, category VARCHAR(25), rating FLOAT, PRIMARY KEY(isbn), FOREIGN KEY(publisher_id) REFERENCES publisher(publisher_id));";
        String sql4 = "CREATE TABLE IF NOT EXISTS phw1(isbn CHAR(13), book_name VARCHAR(20), rating FLOAT);";
        String sql5 = "CREATE TABLE IF NOT EXISTS author_of(isbn CHAR(13), author_id INTEGER, PRIMARY KEY(isbn,author_id), FOREIGN KEY(isbn) REFERENCES book(isbn), " +
                      "FOREIGN KEY(author_id) REFERENCES author(author_id));";
        create_table.add(sql1);
        create_table.add(sql2);
        create_table.add(sql3);
        create_table.add(sql4);
        create_table.add(sql5);

        for(int i=0; i < 5; i++) {
            try {
                Statement statement = this.con.createStatement();
                result = statement.executeUpdate(create_table.get(i));
                table_number++;
                statement.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Created " + table_number +" tables.");
        return table_number;
    }

    public int dropTables(){
        int result;
        int dropped_table_number = 0;
        Vector<String> drop_table = new Vector<>();
        String sql1 = "DROP TABLE IF EXISTS author";
        String sql2 = "DROP TABLE IF EXISTS publisher";
        String sql3 = "DROP TABLE IF EXISTS book";
        String sql4 = "DROP TABLE IF EXISTS author_of";
        String sql5 = "DROP TABLE IF EXISTS phw1";
        drop_table.add(sql1);
        drop_table.add(sql2);
        drop_table.add(sql3);
        drop_table.add(sql4);
        drop_table.add(sql5);

        for(int i=4; i>=0; i--) {
            try {
                Statement statement = this.con.createStatement();
                result = statement.executeUpdate(drop_table.get(i));
                dropped_table_number++;
                statement.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Dropped " + dropped_table_number +" tables.");
        return dropped_table_number;
    }

    public int insertAuthor(Author[] authors)
    {
        int number_inserted = 0;
        int result = 0;
        for(int i=0; i<authors.length; i++)
        {
            Author author = authors[i];
            String sql = "INSERT INTO author VALUES('"
                            + author.getAuthor_id() + "','"
                            + author.getAuthor_name().replaceAll("'","''") + "');";

            try{
                Statement st = this.con.createStatement();
                result = st.executeUpdate(sql);
                number_inserted ++;
                st.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Inserted " + number_inserted +" authors.");
        return number_inserted;
    }

    public int insertBook(Book[] books){
        int number_inserted = 0;
        int result = 0;
        for(int i=0; i<books.length; i++)
        {
            Book book = books[i];
            String sql = "INSERT INTO book VALUES('"
                    + book.getIsbn() + "','"
                    + book.getBook_name().replaceAll("'","''") + "','"
                    + book.getPublisher_id() + "','"
                    + book.getFirst_publish_year() + "','"
                    + book.getPage_count() + "','"
                    + book.getCategory().replaceAll("'","''") + "','"
                    + book.getRating() + "');";

            try{
                Statement st = this.con.createStatement();
                result = st.executeUpdate(sql);
                number_inserted++;
                st.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Inserted " + number_inserted +" books.");
        return number_inserted;
    }

    public int insertPublisher(Publisher[] publishers){
        int number_inserted = 0;
        int result = 0;
        for(int i=0; i<publishers.length; i++)
        {
            Publisher publisher = publishers[i];
            String sql = "INSERT INTO publisher VALUES('"
                    + publisher.getPublisher_id() + "','"
                    + publisher.getPublisher_name().replaceAll("'","''") + "');";

            try{
                Statement st = this.con.createStatement();
                result = st.executeUpdate(sql);
                number_inserted++;
                st.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Inserted " + number_inserted +" publishers.");
        return number_inserted;
    }

    public int insertAuthor_of(Author_of[] author_ofs){
        int number_inserted = 0;
        int result = 0;
        for(int i=0; i<author_ofs.length; i++)
        {
            Author_of author_of = author_ofs[i];
            String sql = "INSERT INTO author_of VALUES('"
                    + author_of.getIsbn() + "','"
                    + author_of.getAuthor_id() + "');";

            try{
                Statement st = this.con.createStatement();
                result = st.executeUpdate(sql);
                number_inserted++;
                st.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Inserted " + number_inserted +" author_ofs.");
        return number_inserted;
    }

    public QueryResult.ResultQ1[] functionQ1(){
        Vector<QueryResult.ResultQ1> arr = new Vector<>();
        QueryResult.ResultQ1[] arr_ret;
        ResultSet rs;
        String sql = "SELECT isbn, first_publish_year, page_count, publisher_name " +
                    "FROM book b, publisher p " +
                    "WHERE b.publisher_id = p.publisher_id AND b.page_count = (SELECT MAX(b1.page_count) FROM book b1) "+
                    "ORDER BY isbn;";
        try{
            Statement st = this.con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ1 result = new QueryResult.ResultQ1("0","1",2,"3");
                result.isbn = rs.getString(1);
                result.first_publish_year = rs.getString(2);
                result.page_count = Integer.parseInt(rs.getString(3));
                result.publisher_name = rs.getString(4);
                arr.addElement(result);
                System.out.println(result);
            }
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ1[arr.size()];
        for(int i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;
    }

    public QueryResult.ResultQ2[] functionQ2(int author_id1, int author_id2){
        Vector<QueryResult.ResultQ2> arr = new Vector<>();
        QueryResult.ResultQ2[] arr_ret;
        ResultSet rs;
        String sql = "SELECT publisher_id, AVG(page_count) " +
                     "FROM book b " +
                     "WHERE b.publisher_id IN " +
                "(SELECT b1.publisher_id " +       //isbn's of books that were co-authored by author1 and author2
                "FROM author_of a1, author_of a2, book b1 " +
                "WHERE a1.author_id = " + Integer.toString(author_id1) + " AND a2.author_id = " + Integer.toString(author_id2) +
                " AND a1.isbn = a2.isbn AND b1.isbn = a1.isbn) " +
                    "GROUP BY b.publisher_id;";
        try{
            Statement st = this.con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ2 result = new QueryResult.ResultQ2(1,0.0);
                result.publisher_id = Integer.parseInt(rs.getString(1));
                result.average_page_count = Double.parseDouble(rs.getString(2));
                arr.addElement(result);
                System.out.println(result);
            }

            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ2[arr.size()];
        for(int i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;
    }

    public QueryResult.ResultQ3[] functionQ3(String author_name){
        Vector<QueryResult.ResultQ3> arr = new Vector<>();
        QueryResult.ResultQ3[] arr_ret;
        ResultSet rs;
        String sql = "SELECT b.book_name, b.category, MIN(b.first_publish_year) " +
                    "FROM author a, author_of ao, book b " +
                    "WHERE a.author_name LIKE '" + author_name + "' AND a.author_id = ao.author_id AND ao.isbn = b.isbn " +
                    "GROUP BY a.author_id, b.isbn " +
                    "ORDER BY b.book_name, b.category, b.first_publish_year;";
        try{
            Statement st = this.con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ3 result = new QueryResult.ResultQ3("0","1","2");
                result.book_name = rs.getString(1);
                result.category = rs.getString(2);
                result.first_publish_year = rs.getString(3);
                arr.addElement(result);
                System.out.println(result);
            }
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ3[arr.size()];
        for(int i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;
    }

    public QueryResult.ResultQ4[] functionQ4(){
        Vector<QueryResult.ResultQ4> arr = new Vector<>();
        QueryResult.ResultQ4[] arr_ret;
        ResultSet rs;
        int j=0;
        String sql ="SELECT DISTINCT publisher_id, category " +
                    "FROM book " +
                    "WHERE publisher_id IN " +

                    "(SELECT DISTINCT p.publisher_id " +
                    "FROM publisher p , book b "+
                    "WHERE p.publisher_name LIKE '%_ _% _%' AND b.publisher_id = p.publisher_id "+
                    "GROUP BY p.publisher_id, p.publisher_name " +
                    "HAVING COUNT(*)>2 AND AVG(rating)>3 )" +
                    "ORDER BY publisher_id, category";
        try{
            Statement st = this.con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ4 result = new QueryResult.ResultQ4(1,"0");
                result.category = rs.getString(2);
                result.publisher_id = Integer.parseInt(rs.getString(1));
                arr.addElement(result);
                System.out.println(result);
            }

            //System.out.println();
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ4[arr.size()];
        int i;
        for(i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;
    }

    public QueryResult.ResultQ5[] functionQ5(int author_id){
        Vector<QueryResult.ResultQ5> arr = new Vector<>();
        QueryResult.ResultQ5[] arr_ret;
        ResultSet rs;
        String sql ="SELECT a.author_id, a.author_name " +
                    "FROM author a " +
                    "WHERE NOT EXISTS " +
                    "(SELECT DISTINCT b1.publisher_id " +
                    "FROM book b1, author_of a1 " +
                    "WHERE a1.author_id = " + author_id + " AND b1.isbn = a1.isbn " +
                    "AND b1.publisher_id NOT IN " +
                    "(SELECT DISTINCT b2.publisher_id " +
                    "FROM book b2, author_of a2 " +
                    "WHERE b2.isbn = a2.isbn AND a2.author_id = a.author_id )) " +
                    "ORDER BY a.author_id ;";
        try{
            Statement st = this.con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ5 result = new QueryResult.ResultQ5(0,"1");
                result.author_id = Integer.parseInt(rs.getString(1));
                result.author_name = rs.getString(2);
                arr.addElement(result);
                System.out.println(result);
            }

            //System.out.println();
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ5[arr.size()];
        int i;
        for(i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;
    }

    public QueryResult.ResultQ6[] functionQ6(){
        Vector<QueryResult.ResultQ6> arr = new Vector<>();
        QueryResult.ResultQ6[] arr_ret;
        ResultSet rs;
        String sql ="SELECT aa.author_id, aa.isbn " +
                    "FROM author_of aa " +
                    "WHERE aa.author_id NOT IN " +
                    "(SELECT a.author_id " +
                    "FROM book b, author_of a " +
                    "WHERE b.isbn = a.isbn AND b.publisher_id IN " +
                    "(SELECT b1.publisher_id " + //publishers who publish from multiple authors
                    "FROM book b1, book b2, author_of a1, author_of a2 " +
                    "WHERE b1.isbn = a1.isbn AND b2.isbn = a2.isbn AND a1.author_id <> a2.author_id AND b1.publisher_id = b2.publisher_id))" +
                    "ORDER BY aa.author_id, aa.isbn";
        try{
            Statement st = this.con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ6 result = new QueryResult.ResultQ6(0,"1");
                result.author_id = Integer.parseInt(rs.getString(1));
                result.isbn = rs.getString(2);
                arr.addElement(result);
                System.out.println(result);
            }

            //System.out.println();
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ6[arr.size()];
        int i;
        for(i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;
    }

    public QueryResult.ResultQ7[] functionQ7(double rating){
        Vector<QueryResult.ResultQ7> arr = new Vector<>();
        QueryResult.ResultQ7[] arr_ret;
        ResultSet rs;
        String sql ="SELECT p.publisher_id, p.publisher_name " +
                    "FROM publisher p, book bb " +
                    "WHERE p.publisher_id = bb.publisher_id AND bb.publisher_id IN " +
                    "(SELECT b.publisher_id " +
                    "FROM book b " +
                    "GROUP BY b.publisher_id " +
                    "HAVING AVG(rating) > " + rating + ")" +
                    "GROUP BY bb.publisher_id, bb.category " +
                    "HAVING bb.category = 'Roman' AND COUNT(*) > 1 " +
                    "ORDER BY bb.publisher_id";

        try{
            Statement st = this.con.createStatement();
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ7 result = new QueryResult.ResultQ7(0,"1");
                result.publisher_id = Integer.parseInt(rs.getString(1));
                result.publisher_name = rs.getString(2);
                arr.addElement(result);
                System.out.println(result);
            }

            //System.out.println();
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ7[arr.size()];
        int i;
        for(i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;
    }

    public QueryResult.ResultQ8[] functionQ8(){
        Vector<QueryResult.ResultQ8> arr = new Vector<>();
        QueryResult.ResultQ8[] arr_ret;
        ResultSet rs;
        String sql_insert ="INSERT INTO phw1(isbn, book_name, rating) " +
                    "SELECT bb.isbn, bb.book_name, bb.rating " +
                    "FROM book bb " +
                    "WHERE bb.rating = " +
                    "(SELECT MIN(b.rating) " +
                    "FROM book b " +
                    "WHERE b.book_name = bb.book_name "+
                    "GROUP BY b.book_name " +
                    "HAVING COUNT(*)>1 AND MIN(b.rating))";
        String sql = "SELECT * FROM phw1 ORDER BY isbn";

        try{
            Statement st = this.con.createStatement();
            st.executeUpdate(sql_insert);
            rs = st.executeQuery(sql);
            while(rs.next()){
                QueryResult.ResultQ8 result = new QueryResult.ResultQ8("0","1",1.1);
                result.isbn = rs.getString(1);
                result.book_name = rs.getString(2);
                result.rating = Double.parseDouble(rs.getString(3));
                arr.addElement(result);
                System.out.println(result);
            }

            //System.out.println();
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        arr_ret = new QueryResult.ResultQ8[arr.size()];
        int i;
        for(i=0; i < arr.size(); i++){
            arr_ret[i] = arr.get(i);
        }
        return arr_ret;    }

    public double functionQ9(String keyword){
        double result = 0.0;
        ResultSet rs;
        String sql_update = "UPDATE book b " +
                            "SET b.rating = b.rating +1 " +
                            "WHERE b.rating <= 4 AND b.book_name LIKE '%" + keyword + "%'";

        String sql = "SELECT SUM(rating) " +
                     "FROM book ";

        try{
            Statement st = this.con.createStatement();
            st.executeUpdate(sql_update);
            rs = st.executeQuery(sql);
            while(rs.next()){
                result = Double.parseDouble(rs.getString(1));
                System.out.println(result);
            }

            //System.out.println();
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    public int function10(){
        int result = 0;
        ResultSet rs;
        String sql_update = "DELETE FROM publisher " +
                            "WHERE publisher_id NOT IN " +
                            "(SELECT b.publisher_id " +
                            "FROM book b )";

        String sql = "SELECT COUNT(*) " +
                "FROM publisher ";

        try{
            Statement st = this.con.createStatement();
            st.executeUpdate(sql_update);
            rs = st.executeQuery(sql);
            while(rs.next()){
                result = Integer.parseInt(rs.getString(1));
                System.out.println(result);
            }

            //System.out.println();
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return result;    }

}
