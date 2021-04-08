package org.versionedentity;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.observe.util.CsvEntitySet;
import org.qommons.IntList;
import org.qommons.collect.QuickSet.QuickMap;

/** A CSV entity set that is synchronized with a version control system */
public abstract class VersionedEntities extends CsvEntitySet {
	// TODO Add ability to query entity history (what, who, where, when)?

	public interface EntityListener {
		void entityUpdated(EntityFormat type, //
			QuickMap<String, Object> oldFields, QuickMap<String, Object> newFields, //
			IntList updatedFields, boolean local);
	}

	// TODO Not nearly robust enough. Include information on who made each revision on each field, where and when
	public interface ConflictResolver {
		QuickMap<String, Object> resolveConflict(EntityFormat format, //
			QuickMap<String, Object> oldEntity, QuickMap<String, Object> theirEntity, QuickMap<String, Object> myEntity, //
			IntList conflictFields);
	}

	public class BranchInfo {
		private final String theBranchName;
		private final int theMajorVersion;
		private final int theMinorVersion;
		private final int thePatchVersion;

		public BranchInfo(String branchName, int majorVersion, int minorVersion, int patchVersion) {
			theBranchName = branchName;
			theMajorVersion = majorVersion;
			theMinorVersion = minorVersion;
			thePatchVersion = patchVersion;
		}

		public String getBranchName() {
			return theBranchName;
		}

		public int getMajorVersion() {
			return theMajorVersion;
		}

		public int getMinorVersion() {
			return theMinorVersion;
		}

		public int getPatchVersion() {
			return thePatchVersion;
		}

		@Override
		public int hashCode() {
			return Objects.hash(theBranchName, theMajorVersion, theMinorVersion, thePatchVersion);
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof BranchInfo//
				&& theBranchName.equals(((BranchInfo) obj).theBranchName)//
				&& theMajorVersion == ((BranchInfo) obj).theMajorVersion//
				&& theMinorVersion == ((BranchInfo) obj).theMinorVersion//
				&& thePatchVersion == ((BranchInfo) obj).thePatchVersion;
		}

		@Override
		public String toString() {
			return new StringBuilder(theBranchName).append('-')//
				.append(theMajorVersion).append('.').append(theMinorVersion).append('.').append(thePatchVersion)//
				.toString();
		}
	}

	/** @see CsvEntitySet#CsvEntities(File) */
	public VersionedEntities(File directory) throws IOException {
		super(directory);
	}

	/**
	 * @param listener The listener to be notified of changes to the current {@link #getBranch() branch} of entities
	 * @return A Runnable to {@link Runnable#run()} to remove the listener from this entity set
	 */
	public abstract Runnable addListener(EntityListener listener);

	/**
	 * <ol>
	 * <li>Checks the remote for changes (and pulls them down, if any)</li>
	 * <li>Merges changes to the current branch with any locally {@link #commit(String) committed} changes</li>
	 * <li>Pushes locally {@link #commit(String) committed} changes to the remote</li>
	 * </ol>
	 * 
	 * @param onConflict Resolves any conflicts
	 * @return This entity set
	 * @throws IOException If an error occurs communicating with the remote or manipulating versions
	 */
	public abstract VersionedEntities checkAndPush(ConflictResolver onConflict) throws IOException;

	/**
	 * Commits all changes locally. Use {@link #checkAndPush(ConflictResolver)} to push the changes to the remote.
	 * 
	 * @param message A description of the changes in the commit. If null is given, a message may be auto-generated.
	 * @return This entity set
	 * @throws IOException If an error occurs committing
	 */
	public abstract VersionedEntities commit(String message) throws IOException;

	/**
	 * @return The current entity branch
	 * @throws IOException If an error occurs retreving the information
	 */
	public abstract BranchInfo getBranch() throws IOException;

	/**
	 * Creates a new branch without committing
	 * 
	 * @param newBranchName The new {@link BranchInfo#getBranchName() name} for the branch, or null to keep the current branch name
	 * @param changeLevel 0 for no change--used for just changing the branch name, 1 for a major revision, 2 for a minor revision, 3 for a
	 *        patch revision
	 * @return This entity set
	 * @throws IOException If an error occurs creating the branch
	 */
	public abstract VersionedEntities branch(String newBranchName, int changeLevel) throws IOException;
}
