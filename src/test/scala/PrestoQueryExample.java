import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PrestoQueryExample {
    public static void main(String[] args) {
        try {
            // JDBC 드라이버 로드
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");

            Properties properties = new Properties();
            properties.setProperty("user", "test");

            // Presto에 연결
            Connection connection = DriverManager.getConnection(
                    "jdbc:presto://3.37.175.245:8889/hive",
                    properties
            );

            // 쿼리 실행
            Statement statement = connection.createStatement();
            String query = "SELECT * FROM  b_competition.name_with_mt20id";
            ResultSet resultSet = statement.executeQuery(query);

            // 결과 처리
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1)+", "+resultSet.getString(2));
                // 결과 가져오기
                // resultSet.getXXX() 메서드를 사용하여 필요한 컬럼 값을 가져올 수 있습니다.
            }

            // 연결 종료
            resultSet.close();
            statement.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
}
