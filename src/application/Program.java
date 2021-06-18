package application;

import java.util.Date;
import model.entities.Department;
import model.entities.Seller;

public class Program {

	public static void main(String[] args) {
		//Connection conn = DB.getConnection();
		//DB.closeConnection();
		
		Department obj = new Department(1, "Teste");
		System.out.println(obj);
		
		Seller seller = new Seller(21, "David", "david@david.com", new Date(), 3000.00, obj);
		System.out.println(seller);
	}
}
