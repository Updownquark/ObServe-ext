package org.observe.ext.util;

import java.awt.EventQueue;
import java.awt.Image;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JProgressBar;

import org.observe.ObservableValue;
import org.observe.SettableValue;
import org.observe.util.swing.JustifiedBoxLayout;
import org.observe.util.swing.WindowPopulation;
import org.qommons.QommonsUtils;
import org.qommons.QuarkVersionUpdater;
import org.qommons.Version;
import org.qommons.io.BetterFile;
import org.qommons.io.FileUtils;
import org.qommons.io.Format;
import org.qommons.json.JsonSerialReader;
import org.qommons.json.JsonSerialReader.JsonParseType;
import org.qommons.json.JsonSerialReader.StructState;
import org.qommons.json.SAJParser;
import org.qommons.threading.QommonsTimer;

/** A class to pull information about published releases for a repository from GitHub */
public class GitHubApiHelper {
	/** The date format used by GitHub */
	public static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = ThreadLocal.withInitial(() -> {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		format.setTimeZone(TimeZone.getTimeZone("GMT"));
		return format;
	});

	private String theGitHubUrl = "https://api.github.com/";
	private String theRepositoryOwner;
	private String theRepository;
	private Pattern theTagPattern;

	/**
	 * @param repoOwner The user name of the owner of the repository
	 * @param repoName The name of the repository
	 */
	public GitHubApiHelper(String repoOwner, String repoName) {
		theRepositoryOwner = repoOwner;
		theRepository = repoName;
	}

	/** @return A pattern matcher to filter releases by tag name */
	public Pattern getTagPattern() {
		return theTagPattern;
	}

	/**
	 * @param tagPattern A pattern matcher to filter releases by tag name
	 * @return This helper
	 */
	public GitHubApiHelper setTagPattern(Pattern tagPattern) {
		theTagPattern = tagPattern;
		return this;
	}

	/**
	 * @param tagPattern A pattern matcher to filter releases by tag name
	 * @return This helper
	 */
	public GitHubApiHelper withTagPattern(String tagPattern) {
		theTagPattern = Pattern.compile(tagPattern);
		return this;
	}

	/**
	 * Contacts the GitHub API for all published releases of the configured application
	 * 
	 * @return The releases available for the configured application
	 * @throws IOException If GitHub could not be reached or an error occurs reading the data
	 */
	public List<Release> getReleases() throws IOException {
		String url = theGitHubUrl;
		url = append(url, "repos/") + theRepositoryOwner + "/" + theRepository + "/releases";
		HttpsURLConnection connection = (HttpsURLConnection) new URL(url).openConnection();
		connection.setRequestProperty("User-Agent", theRepositoryOwner);
		connection.connect();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
			List<Release> releases = new ArrayList<>();
			JsonSerialReader json = new JsonSerialReader(reader);
			json.startArray();
			List<Asset> assets = new ArrayList<>();
			while (json.getNextItem(true, false).getType() == JsonParseType.OBJECT) {
				assets.clear();
				String name = null, tagName = null, htmlUrl = null, desc = null;
				Instant date = null;
				boolean preRelease = false, draft = false;
				String property = json.getNextProperty();
				while (property != null) {
					switch (property) {
					case "html_url":
						htmlUrl = json.parseString();
						break;
					case "name":
						name = json.parseString();
						break;
					case "tag_name":
						tagName = json.parseString();
						break;
					case "body":
						desc = json.parseString();
						break;
					case "draft":
						draft = json.parseBoolean();
						break;
					case "prerelease":
						preRelease = json.parseBoolean();
						break;
					case "published_at":
						date = DATE_FORMAT.get().parse(json.parseString()).toInstant();
						break;
					case "assets":
						parseAssets(json, assets);
						break;
					}

					property = json.getNextProperty();
				}
				if (theTagPattern == null || theTagPattern.matcher(tagName).matches()) {
					releases.add(new Release(name, tagName, htmlUrl, desc, date, draft, preRelease, QommonsUtils.unmodifiableCopy(assets)));
				}
			}
			return releases;
		} catch (SAJParser.ParseException | java.text.ParseException e) {
			throw new IOException("Failed to parse repository list", e);
		}
	}

	/**
	 * @param clazz The class representing the application
	 * @return The latest published release for the application
	 * @throws IOException If GitHub could not be reached or an error occurs reading the data
	 */
	public Release getLatestRelease(Class<?> clazz) throws IOException {
		String currentVersion = getCurrentRelease(clazz);
		boolean preRelease = currentVersion != null && currentVersion.startsWith("0.");
		List<Release> releases = getReleases();
		for (Release r : releases) {
			if (r.isDraft) {
				continue;
			} else if (!preRelease && r.isPreRelease) {
				continue;
			}
			if (!Version.VERSION_PATTERN.matcher(r.getTagName()).matches()) {
				continue;
			}
			if (r.getAssets().isEmpty()) {
				continue;
			}
			return r;
		}
		return null;
	}

	private static String append(String url, String suffix) {
		if (url.charAt(url.length() - 1) != '/') {
			url += "/";
		}
		return url + suffix;
	}

	private static void parseAssets(JsonSerialReader json, List<Asset> assets) throws IOException, SAJParser.ParseException {
		StructState state = json.startArray();
		while (json.getNextItem(true, false).getType() == JsonParseType.OBJECT) {
			String name = null, apiUrl = null, desc = null;
			String property = json.getNextProperty();
			while (property != null) {
				switch (property) {
				case "name":
					name = json.parseString();
					break;
				case "browser_download_url":
					apiUrl = json.parseString();
					break;
				case "label":
					desc = json.parseString();
					break;
				}

				property = json.getNextProperty();
			}
			assets.add(new Asset(name, apiUrl, desc));
		}
		json.endArray(state);
	}

	/**
	 * @param clazz The class representing the application
	 * @return The {@link Package#getImplementationVersion() implementation version} of the {@link Class#getPackage() package} the class
	 *         belongs to, if configured. This information is typically present in the jar manifest
	 */
	public static String getCurrentRelease(Class<?> clazz) {
		Package pkg = clazz.getPackage();
		String currentVersion = null;
		while (pkg != null && (currentVersion = pkg.getImplementationVersion()) == null) {
			String pkgName = pkg.getName();
			int lastDot = pkgName.lastIndexOf('.');
			if (lastDot < 0) {
				break;
			}
			pkg = Package.getPackage(pkgName.substring(0, lastDot));
		}
		return currentVersion;
	}

	/**
	 * @param clazz The class representing the application
	 * @param title The title for the upgrade dialog if a new version is available
	 * @param image The image for the upgrade dialog if a new version is available
	 * @param check A filter to apply to the releases
	 * @param upgradeRejected Code to run if the user chooses NOT to upgrade to the latest release
	 * @param afterCheck Code to run if there is no updated release or the user chooses not to upgrade
	 * @return true if this method was able to compare the current and latest releases, regardless of whether latest was more recent than
	 *         the current or whether the user chose to upgrade
	 * @throws IOException If GitHub could not be reached or an error occurs reading the data
	 */
	public boolean checkForNewVersion(Class<?> clazz, String title, Image image, Predicate<Release> check,
		Consumer<Release> upgradeRejected, Runnable afterCheck) throws IOException {
		return _upgrade(clazz, title, image, check, upgradeRejected, afterCheck, true);
	}

	/**
	 * 
	 * @param clazz The class representing the application
	 * @param title The title for the upgrade dialog
	 * @param image The image for the upgrade dialog
	 * @throws IOException If GitHub could not be reached or an error occurs reading the data
	 */
	public void upgradeToLatest(Class<?> clazz, String title, Image image) throws IOException {
		_upgrade(clazz, title, image, null, null, null, false);
	}

	boolean _upgrade(Class<?> clazz, String title, Image image, Predicate<Release> check, Consumer<Release> upgradeRejected,
		Runnable afterCheck, boolean askUser) throws IOException {
		BetterFile jarFile = FileUtils.getClassFile(clazz);
		while (jarFile != null && !jarFile.getName().toLowerCase().endsWith(".jar")) {
			jarFile = jarFile.getParent();
		}
		if (jarFile == null) {
			if (afterCheck != null) {
				afterCheck.run();
			}
			return false;
		}
		File javaJarFile = new File(jarFile.getPath());
		JFrame[] frame = new JFrame[1];
		try {
			String currentVersion = getCurrentRelease(clazz);
			if (currentVersion == null) {
				return false;
			}
			if (!Version.VERSION_PATTERN.matcher(currentVersion).matches()) {
				return false;
			}
			Version cv = Version.parse(currentVersion);
			List<Release> releases = getReleases();
			Release latest = null;
			Asset asset = null;
			Version rv = null;
			for (Release r : releases) {
				if (r.isDraft) {
					continue;
				} else if (!currentVersion.startsWith("0.") && r.isPreRelease) {
					continue;
				}
				if (!Version.VERSION_PATTERN.matcher(r.getTagName()).matches()) {
					continue;
				}
				rv = Version.parse(r.getTagName());
				for (Asset a : r.getAssets()) {
					if (a.getName().endsWith(".jar")) {
						asset = a;
					}
				}
				if (asset == null) {
					continue;
				}
				latest = r;
				break;
			}
			if (latest == null) {
				return true;
			}
			int comp = cv.compareTo(rv);
			if (comp >= 0) {
				return true;
			}
			if (check != null && !check.test(latest)) {
				return true;
			}
			SettableValue<Boolean> hasChosen = SettableValue.build(boolean.class).withValue(!askUser).safe(false).build();
			Release release = latest;
			String assetUrl = asset.getApiUrl();
			SettableValue<Boolean> canceled = SettableValue.build(boolean.class).withValue(false).build();
			JProgressBar progress = new JProgressBar();
			JButton[] yesButton = new JButton[1];
			frame[0] = WindowPopulation.populateWindow(new JFrame(), null, true, false)//
					.withTitle(title).withIcon(image)//
					.withVContent(content -> {
						content.addLabel(null, ObservableValue.of("A new version of " + title + " is available: " + release.getTagName()),
								Format.TEXT, null);
						content.addLabel(null, hasChosen.map(c -> c ? "Upgrading..." : "Would you like to upgrade?"), Format.TEXT, null);
						progress.setStringPainted(true);
						progress.setIndeterminate(true);
						progress.setString("Downloading new release");
						progress.setVisible(false);
						content.addHPanel(null, new JustifiedBoxLayout(false).mainCenter(), buttons -> {
							buttons.visibleWhen(hasChosen.map(c -> !c));
							buttons.addButton("Yes", __ -> {
								hasChosen.set(true, null);
						}, btn -> yesButton[0] = btn.getEditor());
							buttons.addButton("No", __ -> {
								if (upgradeRejected != null) {
									upgradeRejected.accept(release);
								}
								frame[0].setVisible(false);
							}, null);
						});
						content.addComponent(null, progress, p -> p.fill());
						content.addHPanel(null, new JustifiedBoxLayout(false).mainCenter(), buttons -> buttons.addButton("Cancel", __ -> {
							canceled.set(true, null);
						}, b -> b.visibleWhen(hasChosen).disableWith(canceled.map(c -> c ? "Canceling..." : null))));
					}).run(null).getWindow();
			hasChosen.changes().act(evt -> {
				if (!evt.getNewValue()) {
					return;
				}
				progress.setVisible(true);
				QommonsTimer.getCommonInstance().offload(() -> {
					try {
						doUpgrade(assetUrl, javaJarFile, progress, canceled);
						EventQueue.invokeLater(() -> {
							frame[0].setVisible(false); // User must've canceled
						});
					} catch (IOException e) {
						e.printStackTrace();
						EventQueue.invokeLater(() -> {
							JOptionPane.showMessageDialog(frame[0], title + " could not be updated: " + e.getMessage(), "Update Failed",
									JOptionPane.ERROR_MESSAGE);
							frame[0].setVisible(false);
						});
					}
				});
			});
			frame[0].requestFocus();
			yesButton[0].requestFocus();
			frame[0].setAlwaysOnTop(true);
			frame[0].addComponentListener(new ComponentAdapter() {
				@Override
				public void componentHidden(ComponentEvent e) {
					canceled.set(true, null);
					if (afterCheck != null) {
						afterCheck.run();
					}
				}
			});
			return true;
		} finally {
			if (frame[0] == null || !frame[0].isVisible()) {
				QuarkVersionUpdater.deleteUpdater(javaJarFile);
				if (afterCheck != null) {
					afterCheck.run();
				}
			}
		}
	}

	private static void doUpgrade(String assetUrl, File jarFile, JProgressBar progress, ObservableValue<Boolean> canceled)
		throws IOException {
		File newVersion = new File(jarFile.getAbsoluteFile().getParentFile(),
			jarFile.getName().substring(0, jarFile.getName().length() - 4) + ".updated.jar");
		HttpsURLConnection connection = (HttpsURLConnection) new URL(assetUrl).openConnection();
		boolean redirect;
		do {
			redirect = false;
			switch (connection.getResponseCode()) {
			case 301:
			case 302:
			case 307:
				connection = (HttpsURLConnection) new URL(connection.getHeaderField("Location")).openConnection();
				redirect = true;
				break;
			}
		} while (redirect);
		int length = connection.getContentLength();
		long lastMod = connection.getLastModified();
		EventQueue.invokeLater(() -> {
			progress.setMaximum(length);
			progress.setIndeterminate(false);
		});
		int downloaded = 0;
		try (InputStream in = new BufferedInputStream(connection.getInputStream());
				OutputStream out = new BufferedOutputStream(new FileOutputStream(newVersion))) {
			byte[] buffer = new byte[64 * 1024];
			int read = in.read(buffer);
			while (read >= 0) {
				out.write(buffer, 0, read);
				downloaded += read;
				int d = downloaded;
				EventQueue.invokeLater(() -> progress.setValue(d));
				if (canceled.get()) {
					return;
				}
				read = in.read(buffer);
			}
		}
		if (lastMod > 0) {
			newVersion.setLastModified(lastMod);
		}
		EventQueue.invokeLater(() -> {
			progress.setString("Preparing to install new release");
			progress.setIndeterminate(true);
		});
		new QuarkVersionUpdater().update(jarFile, newVersion);
	}

	/** Represents a release of a software application */
	public static class Release {
		private final String theName;
		private final String theTagName;
		private final String theHtmlUrl;
		private final String theDescription;
		private final Instant thePublishedDate;
		private final boolean isDraft;
		private final boolean isPreRelease;
		private final List<Asset> theAssets;

		Release(String name, String tagName, String htmlUrl, String description, Instant publishedDate, boolean draft, boolean preRelease,
				List<Asset> assets) {
			theName = name;
			theTagName = tagName;
			theHtmlUrl = htmlUrl;
			theDescription = description;
			thePublishedDate = publishedDate;
			isDraft = draft;
			isPreRelease = preRelease;
			theAssets = assets;
		}

		/** @return The name of the release, i.e. its title */
		public String getName() {
			return theName;
		}

		/** @return The name of the tag marking the release--typically a version */
		public String getTagName() {
			return theTagName;
		}

		/** @return The URL for the web page of the release */
		public String getHtmlUrl() {
			return theHtmlUrl;
		}

		/** @return The description of the release */
		public String getDescription() {
			return theDescription;
		}

		/** @return The time the release was published */
		public Instant getPublishedDate() {
			return thePublishedDate;
		}

		/** @return Whether the release is just a draft */
		public boolean isDraft() {
			return isDraft;
		}

		/** @return Whether the release is marked as a pre-release */
		public boolean isPreRelease() {
			return isPreRelease;
		}

		/** @return The {@link Asset asset}s associated with the release */
		public List<Asset> getAssets() {
			return theAssets;
		}

		@Override
		public String toString() {
			return theName;
		}
	}

	/** An asset associated with a release */
	public static class Asset {
		private final String theName;
		private final String theApiUrl;
		private final String theDescription;

		Asset(String name, String apiUrl, String description) {
			theName = name;
			theApiUrl = apiUrl;
			theDescription = description;
		}

		/** @return The file name of the asset */
		public String getName() {
			return theName;
		}

		/** @return The URL at which to download the content of the asset */
		public String getApiUrl() {
			return theApiUrl;
		}

		/** @return A description of the asset */
		public String getDescription() {
			return theDescription;
		}

		@Override
		public String toString() {
			return theName;
		}
	}
}
