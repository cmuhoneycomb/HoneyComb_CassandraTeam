package db.daos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import db.models.Registration;

public class RegistrationDAO {
	static ConnectionPool pool = ConnectionPool.getInstance();
	static RegistrationDAO instance = null;

	public static RegistrationDAO getInstance() {
		if (null == instance) {
			instance = new RegistrationDAO("demo");
		}
		return instance;
	}

	public static RegistrationDAO getNewInstance(String keyspaceName) {
		return new RegistrationDAO(keyspaceName);
	}

	private final String keyspaceName;

	private RegistrationDAO(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	public void createTable() {
		String sql = String
				.format("CREATE TABLE %s.teacher (id bigint PRIMARY KEY,name text, title text,courses list<bigint>)",
						keyspaceName);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	public void dropTable() {
		String sql = String.format("DROP TABLE %s.teacher", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	public void insert(Registration obj) {
		String sql = String
				.format("INSERT INTO %s.teacher(id,name,title,courses) VALUES (?,?,?,?)",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(obj.id, obj.name, obj.title, obj.courses);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}

	public List<Registration> selectAll() {
		String sql = String.format("SELECT * FROM %s.teacher", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		List<Registration> result = new ArrayList<Registration>();
		try {
			ResultSet rs = conn.execute(sql);
			for (Row row : rs) {
				result.add(_constructResult(row));
			}
		} finally {
			conn.close();
		}
		return result;
	}

	public Registration selectById(long id) {
		String sql = String.format("SELECT * FROM %s.teacher WHERE id = ?", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		Registration result = null;
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(id);
			ResultSet rs = conn.execute(bs);
			if (rs.iterator().hasNext())
				result = _constructResult(rs.one());
		} finally {
			conn.close();
		}
		return result;
	}

	private static Registration _constructResult(Row row) {
		Registration obj = new Registration();
		obj.id = row.getLong("id");
		obj.name = row.getString("name");
		obj.title = row.getString("title");
		obj.courses = row.getList("courses", Long.class);
		return obj;
	}

	public static void main(String[] args) {
		ConnectionPool.getInstance().close();
	}
}
