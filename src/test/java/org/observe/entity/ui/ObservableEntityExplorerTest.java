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
import org.observe.entity.jdbc.DbDialect2;
import org.observe.entity.jdbc.DefaultConnectionPool;
import org.observe.entity.jdbc.JdbcEntityProvider4;
import org.observe.entity.jdbc.JdbcTypesSupport;
import org.observe.entity.jdbc.SqlConnector;
import org.observe.entity.jdbc.TableNamingStrategy;
import org.observe.entity.ss.GoogleSheetProvider;
import org.observe.ext.util.GoogleClientUtils;
import org.observe.util.Identified;
import org.observe.util.swing.ObservableSwingUtils;
import org.qommons.ArgumentParsing2;
import org.qommons.Nameable;

import com.google.api.services.sheets.v4.Sheets;

public class ObservableEntityExplorerTest {
	public static ObservableEntityProvider getDefaultProvider(ArgumentParsing2.Arguments args) {
		switch (args.get("provider", ProviderType.class)) {
		case jdbc:
			return getJdbcProvider(args);
		case sheets:
			return getGoogleSheetProvider(args);
		}
		throw new IllegalArgumentException("Unrecognized entity provider type: " + args.get("provider"));
	}

	public static ObservableEntityProvider getJdbcProvider(ArgumentParsing2.Arguments args) {
		try {
			Class.forName(args.get("driver", String.class));
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Could not find H2 DB driver", e);
		}
		Connection connection;
		try {
			connection = DriverManager.getConnection(//
				args.get("url", String.class), args.get("user", String.class), args.get("password", String.class));
		} catch (SQLException e) {
			throw new IllegalStateException("Could not connect to test DB", e);
		}

		DbDialect2 dialect;
		try {
			dialect = Class.forName(args.get("dialect", String.class))//
				.asSubclass(DbDialect2.class)//
				.newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
			| ClassCastException e) {
			throw new IllegalStateException("Could not instantiate dialect " + args.get("dialect"), e);
		}

		return new JdbcEntityProvider4(//
			new DefaultConnectionPool("test", SqlConnector.of(connection)),//
			dialect, //
			TableNamingStrategy.DEFAULT, //
			JdbcTypesSupport.DEFAULT.build(), //
			"PUBLIC"
		);
		// return new JdbcEntityProvider2(new StampedLockingStrategy(null, ThreadConstraint.EDT), //
		// JdbcTypesSupport.DEFAULT.withAutoIncrement("AUTO_INCREMENT").build(), //
		// new H2DbDialect(), //
		// new DefaultConnectionPool("test", SqlConnector.of(connection)), "test", true);
	}

	public static ObservableEntityProvider getGoogleSheetProvider(ArgumentParsing2.Arguments args) {
		Sheets sheetsService;
		try {
			sheetsService = GoogleClientUtils.getSheetsService(GoogleClientUtils.DEFAULT_HOST, new GoogleClientUtils.GoogleClientAccess()//
				.withSheetScope(true)//
				.withAppName("Entity Explorer Test"), args.get("client-id", String.class), args.get("client-secret", String.class),
				"Entity Explorer Test");
		} catch (IOException | GeneralSecurityException e) {
			throw new IllegalStateException(e);
		}
		return new GoogleSheetProvider(sheetsService, args.get("spreadsheet", String.class)).setPrintSheetIdOnCreate(true);
	}

	enum ProviderType {
		jdbc, sheets
	}

	public static void main(String... args) {
		ArgumentParsing2.Arguments parsedArgs = ArgumentParsing2.build()//
			.forValuePattern(p -> p//
				.addEnumArgument("provider", ProviderType.class, a -> a.defaultValue(ProviderType.jdbc))//
				.addStringArgument("driver", a -> a.when("provider", ProviderType.class, //
					c -> c.matches(t -> t.eq(ProviderType.jdbc).not()).forbidden())//
					.defaultValue("org.h2.Driver"))//
				.addStringArgument("dialect", a -> a.when("provider", ProviderType.class, //
					c -> c.matches(t -> t.eq(ProviderType.jdbc).not()).forbidden())//
					.defaultValue("org.observe.entity.jdbc.H2DbDialect2"))//
				.addStringArgument("url", a -> a//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.jdbc).not()).forbidden())//
					.defaultValue("jdbc:h2:./obervableEntityTest"))//
				.addStringArgument("user", a -> a//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.jdbc).not()).forbidden())//
					.defaultValue("h2"))//
				.addStringArgument("password", a -> a//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.jdbc).not()).forbidden())//
					.defaultValue("h2"))//
				.addStringArgument("client-id", a -> a//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.sheets).not()).forbidden())//
					.defaultValue("735222615111-mdtvre1og77j0mp34qr04tt26c33kml7"))//
				.addStringArgument("client-secret", a -> a//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.sheets).not()).forbidden())//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.sheets)).required())//
				)//
				.addStringArgument("spreadsheet", a -> a//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.sheets).not()).forbidden())//
					.when("provider", ProviderType.class, c -> c.matches(t -> t.eq(ProviderType.sheets)).required())//
				)//
			).build()//
			.parse(args);
		ObservableEntityDataSet ds;
		try {
			Duration refreshDuration = Duration.ofMillis(1000);
			ds = ObservableEntityDataSet.build(getDefaultProvider(parsedArgs))//
				.withEntityType(SimpleValue.class).fillFieldsFromClass().build()//
				.withEntityType(SimpleReference.class).fillFieldsFromClass().build()//
				.withEntityType(ValueList.class).fillFieldsFromClass().build()//
				.withEntityType(EntityList.class).fillFieldsFromClass().build()//
				.withEntityType(SubValue.class).withSuper(SimpleValue.class).fillFieldsFromClass().build()//
				.withRefresh(Observable.every(refreshDuration, refreshDuration, null, d -> d, null)).build(Observable.empty());
		} catch (EntityOperationException e) {
			throw new IllegalStateException("Could not set up entity persistence", e);
		}

		ObservableSwingUtils.buildUI()//
			.systemLandF()//
			.withConfig("entity-explorer-test")//
			.withTitle("Observable Entity Explorer Test")//
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
		public SimpleValue getValue();
	}

	public interface ValueList extends Identified, Nameable {
		List<Duration> getDurations();
	}

	public interface EntityList extends Identified, Nameable {
		List<SimpleValue> getValues();
	}

	public interface SubValue extends SimpleValue {
		double getDoubleValue();
	}
}
