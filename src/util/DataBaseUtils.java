package util;

import java.io.Closeable;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DataBaseUtils implements AutoCloseable {

    // 接続文字列、ユーザID、パスワード
    private final String url;
    private final String user;
    private final String password;
    private final boolean autoCommit;
    
    private Statement stmt = null;
    private Connection conn = null;

	public DataBaseUtils(boolean autoConnect, boolean autoCommit) throws SQLException {
		// JDBC接続文字列やユーザ、パスワードの決定。
		// ハードコードとしているが、設定ファイルなど外部ファイルから情報を引用する事が主
		this.url = "jdbc:postgresql://localhost:5432/postgres";
		this.user = "user";
		this.password = "user";
		this.autoCommit = autoCommit;
		if (autoConnect) this.connectDatabase();
	}
	/**
	 * 接続オブジェクトのgetter
	 */
	public Connection getConnection() {
		return this.conn;
	}
	
	/**
	 * DB接続
	 * @return 接続成功時、接続オブジェクト
	 * @throws SQLException
	 */
	public void connectDatabase() throws SQLException {
		this.conn = DriverManager.getConnection(this.url, this.user, this.password);
		this.conn.setAutoCommit(autoCommit);
	}
	
	/**
	 * READ処理の実行
	 * @param sql 実行SQL
	 * @return 結果セット
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String sql) throws SQLException {
		this.closeStatement();
        this.stmt = this.conn.createStatement(); 
        return stmt.executeQuery(sql);
	}
	/**
	 * CREATE,UPDATE,DELETEの実行
	 * @param sql 実行SQL文
	 * @return 更新件数
	 * @throws SQLException
	 */
	public int executeUpdate(String sql) throws SQLException {
		this.closeStatement();
        this.stmt = this.conn.createStatement(); 
        return stmt.executeUpdate(sql);
	}
	private void closeStatement() throws SQLException {
		if (this.stmt != null) { // オブジェクトがある場合
			this.stmt.close();
			this.stmt = null;
		}
	}
	@Override
	public void close() throws SQLException {
		this.closeStatement();
		this.conn.close();
	}

}
