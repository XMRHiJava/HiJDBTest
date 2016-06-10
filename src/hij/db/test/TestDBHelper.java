package hij.db.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import hij.db.DBHelper;
import hij.db.DBOperate;
import hij.db.ICreator;
import hij.db.MySQLCreator;
import hij.util.HiTypeHelper;
import hij.util.generic.HiResult;
import hij.util.generic.IFunc;
import hij.util.generic.IFuncP1;

public class TestDBHelper {

	String url = "jdbc:mysql://localhost:3306/information_schema";
	public TestDBHelper() {
		DBOperate.addDBCreator(DBOperate.MySQL, new IFunc<ICreator>(){

			@Override
			public ICreator handle() {
				// TODO Auto-generated method stub
				return new MySQLCreator();
			}
			
		});
	}
	@Test
	public void Test_DBHelper_ExecuteScalar_String() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
        String obj;
		try {
			obj = db.executeScalar(String.class, "select table_name from tables limit 1");
	        Assert.assertEquals(obj, "CHARACTER_SETS");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void Test_DBHelper_ExecuteScalar_int() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
        int obj;
		try {
			obj = db.executeScalar(int.class, "select Count(1) from tables limit 1");
	        Assert.assertTrue(obj > 0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void Test_DBHelper_ExecuteScalar_Integer() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
        Integer obj;
		try {
			obj = db.executeScalar(int.class, "select Count(1) from tables limit 1");
	        Assert.assertTrue(obj > 0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void Test_DBHelper_ExecuteScalar_String2() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
        int obj;
		try {
			obj = db.executeScalar(int.class, "select table_name from tables where 1 != 1");
	        Assert.assertEquals(obj, -1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void Test_DBHelper_ExecuteScalar_int_null() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
        String obj;
		try {
			obj = db.executeScalar(String.class, "select Count(1) from tables limit 1");
	        Assert.assertTrue(Integer.parseInt(obj) > 0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void Test_DBHelper_ExecuteQuery() {

		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
		try {
			final HiResult<List<CharactersetsPO>> result = new HiResult<List<CharactersetsPO>>();
			boolean ret = db.executeQuery(
					"SELECT  CHARACTER_SET_NAME,  DEFAULT_COLLATE_NAME,  DESCRIPTION,  MAXLEN FROM Character_sets ", 
					new IFuncP1<ResultSet, Boolean>(){
				@Override
				public Boolean handle(ResultSet v) {
					List<CharactersetsPO> lst = null;
					try {
						lst = DBHelper.getResultsList(CharactersetsPO.class, v);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return false;
					}
					result.set(lst);
					return lst != null;
				}
				
			});
			Assert.assertTrue(ret);
			Assert.assertTrue(result.get() != null);
			for (int i = 0; i < result.get().size(); i++) {
				String str = HiTypeHelper.ToString(CharactersetsPO.class, result.get().get(i));
				System.out.print(str);
				System.out.print("\r\n");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void Test_DBHelper_ExecuteQuery_list() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
		List<CharactersetsPO> lst = db.executeQuery(CharactersetsPO.class, 
				"SELECT  CHARACTER_SET_NAME,  DEFAULT_COLLATE_NAME,  DESCRIPTION,  MAXLEN FROM Character_sets ");
		Assert.assertTrue(lst!= null);
		for (int i = 0; i < lst.size(); i++) {
			String str = HiTypeHelper.ToString(CharactersetsPO.class, lst.get(i));
			System.out.print(str);
			System.out.print("\r\n");
		}
	}
	@Test
	public void Test_DBHelper_ExecuteQuery_Single() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
		CharactersetsPO ret =  db.executeQuerySingle(CharactersetsPO.class, "SELECT  CHARACTER_SET_NAME,  DEFAULT_COLLATE_NAME,  DESCRIPTION,  MAXLEN FROM Character_sets limit 1");
		Assert.assertTrue(ret!= null);
		String str = HiTypeHelper.ToString(CharactersetsPO.class, ret);
		System.out.print(str);
		System.out.print("\r\n");
	}
	@Test
	public void Test_DBHelper_ExecuteQuery_Single_null() {
		DBOperate db = new DBOperate(url, "root", "root", DBOperate.MySQL);
		CharactersetsPO ret =  db.executeQuerySingle(CharactersetsPO.class, 
				"SELECT  CHARACTER_SET_NAME,  DEFAULT_COLLATE_NAME,  DESCRIPTION,  MAXLEN FROM Character_sets where 1!=1");
		Assert.assertTrue(ret == null);
		String str = HiTypeHelper.ToString(CharactersetsPO.class, ret);
		Assert.assertEquals(str, "");
	}
}
