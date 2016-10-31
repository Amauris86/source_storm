package storm.sample.twitter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Gestiona la conexion e insercion de registros en una base de
 *         datos MySQL.
 **/
public class MySQLDB {

	Connection con = null;
	Statement stmt = null;
	boolean rs;

	/**
	 * Crea la conexion a la base de datos.
	 */
	public MySQLDB() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			
			//definir la conexion a la base de datos.
			con = DriverManager.getConnection(
					"jdbc:mysql://127.0.0.1:3306/db_twitter_storm", "root",
					"hadoop");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Este metodo permite insertar un nuevo registro en la base de datos.
	 * 
	 * @param idTweet
	 *            Es el id del Tweet procesado.
	 * @param textTweet
	 *            Es el texto del Tweet procesado.
	 * @param pos
	 *            Es el puntaje positivo del Tweet procesado.
	 * @param neg
	 *            Es el puntaje negativo del Tweet procesado.
	 */
	public void create_item(Long idTweet, String textTweet, Float pos,
			Float neg, Integer sentTweet) {
		String query = "INSERT INTO tweet_score " + "VALUES (" + idTweet
				+ ", '" + textTweet + "', " + pos + ", " + neg + ", "
				+ sentTweet + ")";
		try {
			stmt = con.createStatement();
			stmt.executeUpdate(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
