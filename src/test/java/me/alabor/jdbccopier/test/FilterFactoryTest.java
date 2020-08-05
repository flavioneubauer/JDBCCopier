package me.alabor.jdbccopier.test;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import me.alabor.jdbccopier.copier.factory.FilterFactory;

public class FilterFactoryTest {

	@Test
	public void testCreateFilterList() {
		FilterFactory factory = new FilterFactory();
		String input = "this,is,a, test";
		List<String> list = factory.createFilterList(input);

		Assertions.assertEquals(4, list.size());
		Assertions.assertEquals("test", list.get(3));
	}

}
