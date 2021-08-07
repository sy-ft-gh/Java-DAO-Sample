package util;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

/**
 * データベース接続及びSQL実行とトランザクション制御用クラス
 *
 * AutoCloseableを実装しているのでtry-with-resourceでご使用ください。
 */
public class DataBaseUtils implements AutoCloseable {

    // 接続文字列、ユーザID、パスワード
    private final String url;
    private final String user;
    private final String password;
    // 自動コミットの有無
    private final boolean autoCommit;
    
    // 内部処理用ステートメントオブジェクト
	private Statement stmt = null;
	private PreparedStatement pstmt = null;
	// Connectionオブジェクト（）
	private Connection conn = null;

	/**
	 * コンストラクタ
	 * 
	 * @param autoConnect インスタンス生成時に接続を行う場合はtrueを指定します
	 * @param autoCommit  オートコミットモードで接続を行うか指定します。（※通常はfalseで使用する事が望ましいです）
	 * @throws SQLException
	 */
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
	 * ※直接Connectionオブジェクトに介在したい場合はgetterで使用してください。
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
	 * SQLへのパラメータバインディング
	 * @param params パラメータリスト
	 */
	private void setPreparedParams(List<Object> params) throws SQLException {
		int index = 1;
		for (Object param : params) {
			if (param instanceof String) {
				this.pstmt.setString(index, (String)param);
			} else if (param instanceof Integer) {
				this.pstmt.setInt(index, (Integer)param);
			} else if (param instanceof Long) {
				this.pstmt.setLong(index, (Long)param);
			} else if (param instanceof Byte) {
				this.pstmt.setByte(index, (Byte)param);
			} else if (param instanceof Float) {
				this.pstmt.setFloat(index, (Float)param);
			} else if (param instanceof java.util.Date) {
				java.util.Calendar cal = java.util.Calendar.getInstance();
				cal.setTime((java.util.Date)param);
				cal.set(java.util.Calendar.HOUR_OF_DAY, 0);
				cal.set(java.util.Calendar.MINUTE, 0);
				cal.set(java.util.Calendar.SECOND, 0);
				cal.set(java.util.Calendar.MILLISECOND, 0);
				java.sql.Date sqlDate = new java.sql.Date(cal.getTimeInMillis());
				this.pstmt.setDate(index, sqlDate);
			} else {
				throw new SQLException(param.getClass().getName() + "型は未対応です");
			}
			index++;
		}
	}

	/**
	 * READ処理の実行
	 * @param sql 実行SQL
	 * @return 結果セット
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String sql) throws SQLException {
		if (this.conn == null || this.conn.isClosed()) connectDatabase();
		this.closeStatement();
        this.stmt = this.conn.createStatement(); 
        System.out.println("postgres > " + sql);
        return stmt.executeQuery(sql);
	}
	
	/**
	 * READ処理の実行
	 * @param sql 実行SQL
	 * @param param パラメタライズドクエリをする場合に使用。※使用しない場合はnullを指定してください。
	 * @return 結果セット
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String sql, List<Object> param) throws SQLException {
		if (this.conn == null || this.conn.isClosed()) connectDatabase();
		this.closeStatement();
        this.pstmt = this.conn.prepareStatement(sql);
        this.setPreparedParams(param);
        System.out.println("postgres > " + this.pstmt.toString());
        return this.pstmt.executeQuery();
	}
	
	/**
	 * CREATE,UPDATE,DELETEの実行
	 * @param sql 実行SQL文
	 * @return 更新件数
	 * @throws SQLException
	 */
	public int executeUpdate(String sql) throws SQLException {
		if (this.conn == null || this.conn.isClosed()) connectDatabase();
		this.closeStatement();
        this.stmt = this.conn.createStatement(); 
        System.out.println("postgres > " + sql);
        return stmt.executeUpdate(sql);
	}
	
	/**
	 * CREATE,UPDATE,DELETEの実行
	 * @param sql 実行SQL文
	 * @param param SQLへの埋め込みパラメータ
	 * @return 更新件数
	 * @throws SQLException
	 */
	public int executeUpdate(String sql, List<Object> param) throws SQLException {
		if (this.conn == null || this.conn.isClosed()) connectDatabase();
		this.closeStatement();
        this.pstmt = this.conn.prepareStatement(sql); 
        this.setPreparedParams(param);
        System.out.println("postgres > " + this.pstmt.toString());
        return pstmt.executeUpdate();
	}
	/**
	 * コミット実行処理
	 * @throws SQLException
	 */
	public void commitTransaction() throws SQLException {
		try {
			this.conn.commit();
		} catch (SQLException e) {
			// トランザクション処理での例外は重要なため、通常はロギングなどを実施します
			e.printStackTrace();
		}
	}
	/**
	 * ロールバック実行処理
	 * @throws SQLException
	 */
	public void rollbackTransaction() throws SQLException {
		try {
			this.conn.rollback();
		} catch (SQLException e) {
			// トランザクション処理での例外は重要なため、通常はロギングなどを実施します
			e.printStackTrace();
		}
	}
	/**
	 * 作成済みのステートメントを削除
	 * @throws SQLException
	 */
	private void closeStatement() throws SQLException {
		if (this.stmt != null) { // オブジェクトがある場合
			this.stmt.close();
			this.stmt = null;
		}
		if (this.pstmt != null) {
			this.pstmt.close();
			this.pstmt = null;
		}
	}
	/**
	 * クローズ処理をオーバーライドする事でtry-with-resourceに対応
	 */
	@Override
	public void close() throws SQLException {
		this.closeStatement();
		this.conn.close();
	}

}
