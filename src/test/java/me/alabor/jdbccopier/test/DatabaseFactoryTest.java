package me.alabor.jdbccopier.test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import me.alabor.jdbccopier.database.factory.DatabaseFactory;

public class DatabaseFactoryTest {

	private DatabaseFactory factory;

	private final static String DATABASETYPE_MSSQL = "me.alabor.jdbccopier.database.MSSQLDatabase";

	@BeforeEach
	public void prepareFactory() {
		this.factory = new DatabaseFactory();
	}

	@Test
	public void testCreateMSSQLDatabase() {
//		Assertions.assertNotNull(getFactory().createDatabase(DATABASETYPE_MSSQL, ""));
	}

	private DatabaseFactory getFactory() {
		return factory;
	}

}
