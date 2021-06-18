package model.dao;

import java.util.List;
import model.entities.Seller;

public interface SellerDAO {

	void insert(Seller Seller);
	void update(Seller Seller);
	void deleteByID(Integer id);
	Seller findByID(Integer id);
	List<Seller> findAll();	
	
}
