package model.dao.implementation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	public void insert(Seller seller) {
		PreparedStatement st = null;
		String sql = "INSERT " + 
                       "INTO seller (name, email, birthDate, baseSalary, departmentID) " + 
                     "VALUES (?, ?, ?, ?, ?)";
		
		try {
			st = conn.prepareStatement( sql, Statement.RETURN_GENERATED_KEYS );
			st.setString(1, seller.getName());
			st.setString(2, seller.getEmail());
			st.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			st.setDouble(4, seller.getBaseSalary());
			st.setInt(5, seller.getDepartment().getID());
			
			int rowsAffected = st.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()){
					int id = rs.getInt(1);
					seller.setID(id);
				}
				
				DB.closeResultSet(rs);
			}
			else {
				throw new DBException("Erro inesperado: Nenhum registro inserido!");
			}
		}	
		catch (SQLException e) {
			throw new DBException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Seller seller) {
		PreparedStatement st = null;
		String sql = "UPDATE seller " + 
		                "SET name = ?, " +
				            "email = ?, " +
		                    "birthDate = ?, " +
				            "baseSalary = ?, " + 
				            "departmentID = ? " + 
                      "WHERE ID = ?";
		
		try {
			st = conn.prepareStatement( sql );
			st.setString(1, seller.getName());
			st.setString(2, seller.getEmail());
			st.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			st.setDouble(4, seller.getBaseSalary());
			st.setInt(5, seller.getDepartment().getID());
			st.setInt(6, seller.getID());
			st.executeUpdate();
		}	
		catch (SQLException e) {
			throw new DBException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteByID(Integer id) {
		PreparedStatement st = null;
		String sql = "DELETE FROM seller WHERE ID = ?";
		
		try {
			st = conn.prepareStatement( sql );
			st.setInt(1, id);
			st.executeUpdate();
		}	
		catch (SQLException e) {
			throw new DBException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
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
			
			// A primeira posição do result set não contém dados, por isso, utiliza-se o next,
			// para saber se existe ao menos um registro.
			if (rs.next()) {
				Department dep = instantiateDepartment(rs);
				Seller sel     = instantiateSeller(rs, dep); 
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

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller sel = new Seller();
		sel.setID(rs.getInt("ID"));
		sel.setName(rs.getString("name"));
		sel.setEmail(rs.getString("email"));
		sel.setBaseSalary(rs.getDouble("baseSalary"));
		sel.setBirthDate(rs.getDate("birthDate"));
		sel.setDepartment(dep);
		return sel;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setID(rs.getInt("departmentID"));
		dep.setName(rs.getString("depName"));		
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		String sql = "SELECT s.*,"                   +
				            "d.name as depName "     +
				       "FROM seller s "              +
				 "INNER JOIN department d "          +
				         "ON s.departmentID = d.ID " +
				   "ORDER BY name";
		
		try {
			st = conn.prepareStatement( sql );
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();
			
			// A primeira posição do result set não contém dados, por isso, utiliza-se o next,
			// para saber se existe ao menos um registro.
			while (rs.next()) {
				Department dep = map.get(rs.getInt("departmentID"));
				
				if ( dep == null ) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("departmentID"), dep);
				}
				
				Seller sel = instantiateSeller(rs, dep); 
				list.add(sel);
			} 
			
			return list;
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
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		String sql = "SELECT s.*,"                   +
				            "d.name as depName "     +
				       "FROM seller s "              +
				 "INNER JOIN department d "          +
				         "ON s.departmentID = d.ID " +
				      "WHERE departmentID = ? "      +
				   "ORDER BY name";
		
		try {
			st = conn.prepareStatement( sql );
			st.setInt(1, department.getID());
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();
			
			// A primeira posição do result set não contém dados, por isso, utiliza-se o next,
			// para saber se existe ao menos um registro.
			while (rs.next()) {
				Department dep = map.get(rs.getInt("departmentID"));
				
				if ( dep == null ) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("departmentID"), dep);
				}
				
				Seller sel = instantiateSeller(rs, dep); 
				list.add(sel);
			} 
			
			return list;
		}	
		catch (SQLException e) {
			throw new DBException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}
}