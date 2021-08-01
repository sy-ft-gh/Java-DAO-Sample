import java.util.List;

import model.Employee;
import dao.EmployeeDAO;

public class SelectEmployeeSample {
  public static void main(String[] args) {

    // employeeテーブルの全レコードを取得
    EmployeeDAO empDAO = new EmployeeDAO();
    List<Employee> empList = empDAO.findAll();

    // 取得したレコードの内容を出力
    for (Employee emp : empList) {
      System.out.println("ID:" + emp.getId());
      System.out.println("名前:" + emp.getName());
      System.out.println("年齢:" + emp.getAge() + "\n");
    }
  }
}