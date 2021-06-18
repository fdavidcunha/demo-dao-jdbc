package model.dao;

import java.util.List;
import model.entities.Department;

public interface DepartmentDAO {

	void insert(Department department);
	void update(Department department);
	void deleteByID(Integer id);
	Department findByID(Integer id);
	List<Department> findAll();	
}
