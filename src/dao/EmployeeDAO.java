package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import model.Employee;
import util.DataBaseUtils;

public class EmployeeDAO {
  public List<Employee> findAll() {
    List<Employee> empList = new ArrayList<Employee>();

    try (DataBaseUtils du = new DataBaseUtils(true, false)) {
        // SELECT文を準備
        String sql = "SELECT ID, NAME, AGE FROM EMPLOYEE";
        // SELECTを実行し、結果表を取得
        ResultSet rs = du.executeQuery(sql);
    	
        // 結果表に格納されたレコードの内容を
        // Employeeインスタンスに設定し、ArrayListインスタンスに追加
        while (rs.next()) {
          String id = rs.getString("ID");
          String name = rs.getString("NAME");
          int age = rs.getInt("AGE");
          Employee employee = new Employee(id, name, age);
          empList.add(employee);
        }
    } catch(SQLException ex) {
        ex.printStackTrace();
        return null;
    }
   return empList;
  }
}