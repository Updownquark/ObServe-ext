package org.observe.ext.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.swing.JOptionPane;

import com.google.common.io.Files;

public class QuarkVersionUpdater {
	private static final long MAX_DELAY_TIME = 5000;

	private static final String UPDATER_MANIFEST = "Manifest-Version: 1.0"//
			+ "\nMain-Class: " + QuarkVersionUpdater.class.getName()//
			+ "\nClass-Path: ." //
			+ "\n";

	public static void main(String... args) {
		File oldVersion = new File(args[0]);
		File newVersion = new File(args[1]);
		long start = System.currentTimeMillis();
		IOException ex = null;
		boolean success = false;
		do {
			if (!oldVersion.exists() || oldVersion.delete()) {
				try {
					Files.move(newVersion, oldVersion);
					success = true;
				} catch (IOException e) {
					ex = e;
				}
			}
		} while (!success && System.currentTimeMillis() - start < MAX_DELAY_TIME);
		if (success) {
			try {
				new ProcessBuilder("java", "-Dshow.app.version=true", "-jar", args[0]).start();
			} catch (IOException e) {
				e.printStackTrace();
				JOptionPane.showMessageDialog(null, "Could not restart " + args[0], "Could not restart app", JOptionPane.ERROR_MESSAGE);
			}
		} else {
			if (ex != null) {
				ex.printStackTrace();
			}
			StringBuilder msg = new StringBuilder();
			msg.append("Unable to replace the application with updated version.<br>");
			if (oldVersion.exists()) {
				msg.append("Please delete ").append(oldVersion.getAbsolutePath()).append("\n and then ");
			} else {
				msg.append("Please ");
			}
			msg.append("rename ").append(newVersion.getAbsolutePath()).append(" to ").append(oldVersion.getName());
			JOptionPane.showMessageDialog(null, "Update partial failure", msg.toString(), JOptionPane.ERROR_MESSAGE);
		}
	}

	public static void update(File jarFile, File updatedJarFile) throws IOException, IllegalStateException {
		// Extract this class into a standalone class file
		File standalone = new File(jarFile.getParentFile(), QuarkVersionUpdater.class.getSimpleName() + ".jar");
		String classFileName = QuarkVersionUpdater.class.getName();
		int dotIdx = classFileName.lastIndexOf('.');
		if (dotIdx >= 0) {
			classFileName = classFileName.substring(dotIdx + 1);
		}
		classFileName += ".class";
		URL resource = QuarkVersionUpdater.class.getResource(classFileName);
		if (resource == null) {
			throw new IllegalStateException("Could not find class file for " + QuarkVersionUpdater.class.getName());
		}
		String classPath = QuarkVersionUpdater.class.getName().replaceAll("\\.", "/") + ".class";
		try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(standalone))) {
			zip.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zip));
			writer.append(UPDATER_MANIFEST);
			writer.flush();
			zip.putNextEntry(new ZipEntry(classPath));
			copy(resource.openStream(), zip);
		}

		List<String> args = new ArrayList<>();
		args.add("java");
		// args.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8001");
		args.add("-jar");
		args.add(standalone.getPath());
		args.add(jarFile.getPath());
		args.add(updatedJarFile.getPath());
		Process process = new ProcessBuilder(args).redirectOutput(Redirect.INHERIT).redirectError(Redirect.INHERIT).start();
		Thread delay = new Thread(() -> {
			while (true) {
				try {
					process.getOutputStream().write(1);
					process.getOutputStream().flush();
				} catch (IOException e) {
				}
				try {
					Thread.sleep(5);
				} catch (InterruptedException e) {
				}
			}
		}, QuarkVersionUpdater.class.getSimpleName() + " Delay");
		delay.start();
		System.exit(0);
	}

	public static void deleteUpdater(File jarFile) {
		File standalone = new File(jarFile.getParentFile(), QuarkVersionUpdater.class.getSimpleName() + ".jar");
		if (standalone.exists()) {
			standalone.delete();
		}
	}

	public static void copy(InputStream in, File target) throws IOException {
		try (BufferedInputStream bufferedIn = new BufferedInputStream(in); //
				OutputStream out = new BufferedOutputStream(new FileOutputStream(target))) {
			copy(bufferedIn, out);
		}
	}

	static void copy(InputStream in, OutputStream out) throws IOException {
		byte[] buffer = new byte[64 * 1028];
		int read = in.read(buffer);
		while (read >= 0) {
			out.write(buffer, 0, read);
			read = in.read(buffer);
		}
	}
}
