package model.dao.implementation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import db.DB;
import db.DBException;
import model.dao.SellerDAO;
import model.entities.Department;
import model.entities.Seller;

public class SellerDAOJDBC implements SellerDAO{

	private Connection conn;
	
	public SellerDAOJDBC(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void insert(Seller Seller) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(Seller Seller) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteByID(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findByID(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		String sql = "SELECT s.*,"               + 
                            "d.name as depName " +
                       "FROM seller s "          + 
                 "INNER JOIN department d "      +
                    "ON s.departmentID = d.ID "  +
                 "WHERE s.ID = ?";
		
		try {
			st = conn.prepareStatement( sql );
			st.setInt(1, id);
			rs = st.executeQuery();
			
			// A primeira posi��o do result set n�o cont�m dados, por isso, utiliza-se o next,
			// para saber se existe ao menos um registro.
			if (rs.next()) {
				Department dep = new Department();
				dep.setID(rs.getInt("departmentID"));
				dep.setName(rs.getString("depName"));
				
				Seller sel = new Seller();
				sel.setID(rs.getInt("ID"));
				sel.setName(rs.getString("name"));
				sel.setEmail(rs.getString("email"));
				sel.setBaseSalary(rs.getDouble("baseSalary"));
				sel.setBirthDate(rs.getDate("birthDate"));
				sel.setDepartment(dep);
				
				return sel;
			} else {
				return null;
			}
		}	
		catch (SQLException e) {
			throw new DBException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

}
