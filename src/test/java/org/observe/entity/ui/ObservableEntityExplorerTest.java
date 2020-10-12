package org.observe.entity.ui;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.observe.Observable;
import org.observe.ObservableValue;
import org.observe.entity.EntityOperationException;
import org.observe.entity.ObservableEntityDataSet;
import org.observe.entity.ObservableEntityProvider;
import org.observe.entity.jdbc.DefaultConnectionPool;
import org.observe.entity.jdbc.H2DbDialect;
import org.observe.entity.jdbc.JdbcEntityProvider2;
import org.observe.entity.jdbc.JdbcEntitySupport;
import org.observe.entity.jdbc.SqlConnector;
import org.observe.entity.ss.GoogleSheetProvider;
import org.observe.ext.util.GoogleClientUtils;
import org.observe.util.Identified;
import org.observe.util.swing.ObservableSwingUtils;
import org.qommons.ArgumentParsing;
import org.qommons.Nameable;
import org.qommons.collect.StampedLockingStrategy;

import com.google.api.services.sheets.v4.Sheets;

public class ObservableEntityExplorerTest {
	public static ObservableEntityProvider getDefaultProvider(ArgumentParsing.Arguments args) {
		return getGoogleSheetProvider(args);
	}

	public static JdbcEntityProvider2 getJdbcProvider(ArgumentParsing.Arguments args) {
		try {
			Class.forName("org.h2.Driver");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Could not find H2 DB driver", e);
		}
		Connection connection;
		try {
			connection = DriverManager.getConnection(args.getString("url"), args.getString("user"), args.getString("password"));
		} catch (SQLException e) {
			throw new IllegalStateException("Could not connect to test DB", e);
		}
		return new JdbcEntityProvider2(new StampedLockingStrategy(null), //
				JdbcEntitySupport.DEFAULT.withAutoIncrement("AUTO_INCREMENT").build(), //
				new H2DbDialect(), //
				new DefaultConnectionPool("test", SqlConnector.of(connection)), "test", true);
	}

	public static GoogleSheetProvider getGoogleSheetProvider(ArgumentParsing.Arguments args) {
		Sheets sheetsService;
		try {
			sheetsService = GoogleClientUtils.getSheetsService(GoogleClientUtils.DEFAULT_HOST, new GoogleClientUtils.GoogleClientAccess()//
							.withSheetScope(true)//
							.withAppName("Entity Explorer Test"),
					args.getString("client-id"), args.getString("client-secret"), "Entity Explorer Test");
		} catch (IOException | GeneralSecurityException e) {
			throw new IllegalStateException(e);
		}
		return new GoogleSheetProvider(sheetsService, args.getString("spreadsheet")).setPrintSheetIdOnCreate(true);
	}

	enum ProviderType {
		jdbc, sheets
	}

	public static void main(String... args) {
		ArgumentParsing.Arguments parsedArgs = ArgumentParsing.create().forDefaultPattern()//
				.enumArg("provider", ProviderType.class).defValue(ProviderType.jdbc)//
				.stringArg("url").defValue("jdbc:h2:./obervableEntityTest")//
				.stringArg("user").defValue("h2")//
				.stringArg("password").defValue("h2")//
				.stringArg("client-id").defValue("735222615111-mdtvre1og77j0mp34qr04tt26c33kml7")//
				.stringArg("client-secret").requiredIf("provider", ProviderType.sheets)//
				.stringArg("spreadsheet")//
				.getArgPattern().getParser().parse(args);
		ObservableEntityDataSet ds;
		try {
			Duration refreshDuration = Duration.ofMillis(1000);
			ds = ObservableEntityDataSet
					.build(getDefaultProvider(parsedArgs))//
				.withEntityType(SimpleValue.class).fillFieldsFromClass().build()//
				.withEntityType(SimpleReference.class).fillFieldsFromClass().build()//
				.withEntityType(ValueList.class).fillFieldsFromClass().build()//
				.withEntityType(EntityList.class).fillFieldsFromClass().build()//
				.withEntityType(SubValue.class).withSuper(SimpleValue.class).fillFieldsFromClass().build()//
				.withRefresh(Observable.every(refreshDuration, refreshDuration, null, d -> d, null))
				.build(Observable.empty());
		} catch (EntityOperationException e) {
			throw new IllegalStateException("Could not set up entity persistence", e);
		}

		ObservableSwingUtils.buildUI()//
		.withConfig("entity-explorer-test").withConfigAt("observableEntityTest.config")//
		.withTitle("Observable Entity Explorer Test")//
		.systemLandF()//
		.build(config -> {
			ObservableEntityExplorer explorer = new ObservableEntityExplorer(config, ObservableValue.of(ds));
			return explorer;
		});
	}

	/* TODO
	 * Entity references
	 * Entity inheritance
	 * Entity multiple inheritance
	 * A, B extends A, C extends A, D extends B, C
	 * Collections of values
	 * Collections of entities
	 * value sets
	 * entity sets
	 * maps
	 * multi maps
	 */

	public interface SimpleValue extends Identified, Nameable {
		public int getValue();
	}

	public interface SimpleReference extends Identified, Nameable {
		public SimpleValue getDuration();
	}

	public interface ValueList extends Identified, Nameable {
		List<Duration> getDurations();
	}

	public interface EntityList extends Identified, Nameable {
		List<SimpleValue> getDurations();
	}

	public interface SubValue extends SimpleValue {
		double getDoubleValue();
	}
}
