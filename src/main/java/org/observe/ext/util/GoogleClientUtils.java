package org.observe.ext.util;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.qommons.QommonsUtils;
import org.qommons.json.JsonSerialWriter;
import org.qommons.json.JsonStreamWriter;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;

public class GoogleClientUtils {
	public static class GoogleClientHost {
		public final String host;
		public final String authUri;
		public final String tokenUri;
		public final String providerCertUrl;
		public final List<String> redirectUris;

		public GoogleClientHost(String host, String authUri, String tokenUri, String providerCertUrl, List<String> redirectUris) {
			this.host = host;
			this.authUri = authUri;
			this.tokenUri = tokenUri;
			this.providerCertUrl = providerCertUrl;
			this.redirectUris = QommonsUtils.unmodifiableCopy(redirectUris);
		}
	}

	public static class GoogleClientAccess {
		private String theAccessType;
		private final List<String> theScopes;
		private int thePort;
		private File theDataStoreDirectory;

		private String theApplicationName;

		public GoogleClientAccess() {
			theScopes = new ArrayList<>();
			thePort = 8888;
			theDataStoreDirectory = new File("tokens");
		}

		public String getAccessType() {
			return theAccessType;
		}

		public List<String> getScopes() {
			return theScopes;
		}

		public int getPort() {
			return thePort;
		}

		public File getDataStoreDirectory() {
			return theDataStoreDirectory;
		}

		public String getAppName() {
			return theApplicationName;
		}

		public GoogleClientAccess withAccessType(String accessType) {
			theAccessType = accessType;
			return this;
		}

		public GoogleClientAccess online(boolean online) {
			theAccessType = online ? "online" : "offline";
			return this;
		}

		public GoogleClientAccess withScope(String scope) {
			theScopes.add(scope);
			return this;
		}

		public GoogleClientAccess withSheetScope(boolean withWrite) {
			return withScope(withWrite ? SheetsScopes.SPREADSHEETS : SheetsScopes.SPREADSHEETS_READONLY);
		}

		public GoogleClientAccess withPort(int port) {
			thePort = port;
			return this;
		}

		public GoogleClientAccess withDataStore(String dataStorePath) {
			theDataStoreDirectory = new File(dataStorePath);
			return this;
		}

		public GoogleClientAccess withDataStore(File dataStore) {
			theDataStoreDirectory = dataStore;
			return this;
		}

		public GoogleClientAccess withAppName(String appName) {
			theApplicationName = appName;
			return this;
		}
	}

	public static final String CREDENTIALS_TEMPLATE_PATH = "credentials-template.json";
	public static final String CREDENTIAL_ROOT = "installed";
	public static final String CLIENT_ID = "client_id";
	public static final String PROJECT_ID = "project_id";
	public static final String AUTH_URI = "auth_uri";
	public static final String TOKEN_URI = "token_uri";
	public static final String AUTH_PROVIDER_URL = "auth_provider_x509_cert_url";
	public static final String CLIENT_SECRET = "client_secret";
	public static final String REDIRECT_URIS = "redirect_uris";
	public static final GoogleClientHost DEFAULT_HOST = new GoogleClientHost(//
			"apps.googleusercontent.com", //
			"https://accounts.google.com/o/oauth2/auth", //
			"https://oauth2.googleapis.com/token", //
			"https://www.googleapis.com/oauth2/v1/certs", //
			Arrays.asList("urn:ietf:wg:oauth:2.0:oob", "http://localhost"));

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	public static String createCredentialJson(GoogleClientHost host, String clientId, String clientSecret, String projectId) {
		try (StringWriter writer = new StringWriter()) {
			JsonSerialWriter json = new JsonStreamWriter(writer).setFormal(true);
			json.startObject().startProperty(CREDENTIAL_ROOT).startObject();

			json.startProperty(CLIENT_ID).writeString(clientId + "." + host.host);
			json.startProperty(PROJECT_ID).writeString(projectId);
			json.startProperty(AUTH_URI).writeString(host.authUri);
			json.startProperty(TOKEN_URI).writeString(host.tokenUri);
			json.startProperty(AUTH_PROVIDER_URL).writeString(host.providerCertUrl);
			json.startProperty(CLIENT_SECRET).writeString(clientSecret);
			json.startProperty(REDIRECT_URIS).startArray();
			for (String redirect : host.redirectUris) {
				json.writeString(redirect);
			}
			json.endArray();

			json.endObject().endObject();
			return writer.toString();
		} catch (IOException e) {
			throw new IllegalStateException("IO Exception from a StringWriter?", e);
		}
	}

	public static Credential authorize(GoogleClientHost host, GoogleClientAccess access, String clientId, String clientSecret,
			String projectId, NetHttpTransport httpTransport) throws IOException {
		String credentialJson = createCredentialJson(host, clientId, clientSecret, projectId);
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new StringReader(credentialJson));

		// Build flow and trigger user authorization request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, clientSecrets, //
				access.getScopes())//
						.setDataStoreFactory(new FileDataStoreFactory(access.getDataStoreDirectory()))//
						.setAccessType(access.getAccessType()).build();
		LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(access.getPort()).build();
		return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
	}

	public static Sheets getSheetsService(GoogleClientHost host, GoogleClientAccess access, String clientId, String clientSecret,
			String projectId) throws IOException, GeneralSecurityException {
		NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		Credential cred = authorize(host, access, clientId, clientSecret, projectId, httpTransport);
		return new Sheets.Builder(httpTransport, JSON_FACTORY, cred).setApplicationName(access.getAppName()).build();
	}
}
