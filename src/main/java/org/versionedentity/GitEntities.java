package org.versionedentity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LogCommand;
import org.eclipse.jgit.api.MergeResult;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.diff.DiffFormatter;
import org.eclipse.jgit.diff.RawTextComparator;
import org.eclipse.jgit.lib.BranchTrackingStatus;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.merge.MergeStrategy;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.FetchResult;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.TrackingRefUpdate;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.util.io.DisabledOutputStream;
import org.observe.util.VersionedEntities;
import org.qommons.collect.CircularArrayList;
import org.qommons.collect.ListenerList;
import org.qommons.collect.QuickSet.QuickMap;
import org.qommons.io.CsvParser;
import org.qommons.io.TextParseException;

/** {@link VersionedEntities} implementation backed by JGit */
public class GitEntities extends VersionedEntities {
	static class ChangeListenerHolder {
		final ChangeListener listener;
		final boolean remoteOnly;

		ChangeListenerHolder(ChangeListener listener, boolean remoteOnly) {
			this.listener = listener;
			this.remoteOnly = remoteOnly;
		}
	}

	private final Git theGit;
	private final CredentialsProvider theCredentials;
	private final File theRepoRoot;
	private ListenerList<ChangeListenerHolder> theListeners;
	private int theLocalListeners;
	private boolean hasModifications;
	private boolean isFreshBranch;

	/**
	 * @param git The git repository to manage entities in
	 * @param indexDirectory The directory in which to put entity index files
	 * @param projectPath The path within the repository to the entity set
	 * @param credentials The credentials to use to communicate with the remote
	 * @throws IOException If an error occurs scanning the directory for entity types
	 */
	public GitEntities(Git git, File indexDirectory, String projectPath, CredentialsProvider credentials) throws IOException {
		super(getProjectDir(git, projectPath), indexDirectory);
		theGit = git;
		theCredentials = credentials;
		theRepoRoot = theGit.getRepository().getDirectory().getParentFile();
		theListeners = ListenerList.build().build();
		isFreshBranch = getEntityTypes().isEmpty();
		addListener(commit -> {
			for (EntityUpdate update : commit.getChanges()) {
				try {
					if (update.getOldValues() == null) {
						updateIndex(update.getEntityType(), update.getNewValues(), true, ((EntityUpdateImpl) update).getFileIndex());
					} else if (update.getNewValues() == null) {
						updateIndex(update.getEntityType(), update.getOldValues(), false, ((EntityUpdateImpl) update).getFileIndex());
					}
				} catch (IOException e) {
					System.err.println("Could not update " + update.getEntityType().getName());
					e.printStackTrace();
				}
			}
		}, true);
	}

	private static File getProjectDir(Git git, String projectPath) {
		File gitDir = git.getRepository().getDirectory().getParentFile();
		return (projectPath == null || projectPath.isEmpty()) ? gitDir : new File(gitDir, projectPath);
	}

	@Override
	public Runnable addListener(ChangeListener listener, boolean remoteOnly) {
		Runnable remove = theListeners.add(new ChangeListenerHolder(listener, remoteOnly), true);
		if (remoteOnly) {
			return remove;
		}
		theLocalListeners++;
		boolean[] removed = new boolean[1];
		return () -> {
			if (!removed[0]) {
				removed[0] = true;
				theLocalListeners--;
				remove.run();
			}
		};
	}

	@Override
	public VersionedEntities checkAndPush(ConflictResolver onConflict) throws IOException, IllegalStateException {
		if (hasModifications) {
			throw new IllegalStateException("Checking and pushing with uncommitted changes");
		}
		String branch = theGit.getRepository().getFullBranch();
		if (branch == null) {
			throw new IllegalStateException("Not checked out to a branch");
		}
		String remoteName = theGit.getRepository().getRemoteNames().iterator().next();
		String shortBranch = theGit.getRepository().getBranch();
		BranchTrackingStatus status = BranchTrackingStatus.of(theGit.getRepository(), branch);
		if (status == null) {
			if (theGit.getRepository().getRefDatabase().findRef(branch) == null) {
				try {
					theGit.branchCreate()//
						// .setUpstreamMode(SetupUpstreamMode.TRACK)//
						.setName(shortBranch)//
						.setForce(true)//
						// .setStartPoint(remoteName + "/" + shortBranch)//
						.call();
				} catch (GitAPIException e) {
					throw new IOException("Could not create branch " + branch, e);
				}
			}
		}
		if (status != null) {
			FetchResult fetch;
			try {
				fetch = theGit.fetch().setCredentialsProvider(theCredentials).call();
			} catch (GitAPIException e) {
				throw new IOException("Unable to fetch", e);
			}
			Ref previousHead = theGit.getRepository().getRefDatabase().findRef(Constants.HEAD);
			TrackingRefUpdate update = fetch.getTrackingRefUpdate(status.getRemoteTrackingBranch());
			if (update != null) {
				switch (update.getResult()) {
				case NOT_ATTEMPTED:
					return this; // Ok, canceled somehow
				case IO_FAILURE:
				case LOCK_FAILURE:
				case REJECTED:
				case REJECTED_CURRENT_BRANCH:
				case REJECTED_MISSING_OBJECT:
				case REJECTED_OTHER_REASON:
					throw new IOException("Fetch failed: " + update.getResult());
				case RENAMED:
					throw new IOException("Remote branch renamed");
				case FAST_FORWARD:
				case FORCED:
					// Try to merge into local branch
					MergeResult mergeResult;
					try {
						mergeResult = theGit.merge().setStrategy(MergeStrategy.RESOLVE).include(update.getNewObjectId()).call();
					} catch (GitAPIException e) {
						throw new IOException("Merge failed", e);
					}
					switch (mergeResult.getMergeStatus()) {
					case ABORTED:
						return this;
					case FAILED:
					case NOT_SUPPORTED:
						throw new IOException("Merge failed: " + mergeResult.getMergeStatus());
					case CHECKOUT_CONFLICT:
					case CONFLICTING:
						// TODO Resolve merge conflicts
						throw new IllegalStateException("Conflict resolution not yet supported");
						//$FALL-THROUGH$ Still need to merge and fire listeners
					case MERGED:
					case MERGED_NOT_COMMITTED:
					case MERGED_SQUASHED:
					case MERGED_SQUASHED_NOT_COMMITTED:
						try {
							theGit.commit().setMessage("Auto-merge from remote: " + mergeResult.getMergeStatus()).call();
						} catch (GitAPIException e) {
							throw new IOException("Could not commit post=merge", e);
						}
						//$FALL-THROUGH$ Still need to fire listeners
					case FAST_FORWARD:
					case FAST_FORWARD_SQUASHED:
						if (!theListeners.isEmpty()) { // Determine what changed and fire listeners
							RevCommit oldHead, newHead;
							try {
								LogCommand log = theGit.log()//
									// .add(previousHead.getObjectId())
									.add(theGit.getRepository().getRefDatabase().findRef(Constants.HEAD).getObjectId());
								// for (ObjectId merged : mergeResult.getMergedCommits()) {
								// log.add(merged);
								// }
								Iterator<RevCommit> logs = log.call().iterator();
								// Logs are sorted most recent first
								newHead = logs.next();
								oldHead = null;
								while (logs.hasNext()) {
									oldHead = logs.next();
									if (oldHead.equals(previousHead.getObjectId())) {
										break;
									}
								}
							} catch (GitAPIException e) {
								throw new IOException("Could not query change commits", e);
							}
							// Find a direct path between previousHead and newHead
							List<RevCommit> path = findPath(oldHead, newHead);
							RevCommit previous = oldHead;
							CanonicalTreeParser parser1 = new CanonicalTreeParser();
							CanonicalTreeParser parser2 = new CanonicalTreeParser();
							try (ObjectReader reader = theGit.getRepository().newObjectReader()) {
								parser1.reset(reader, theGit.getRepository().resolve(previous.name() + "^{tree}"));
								for (RevCommit merged : path) {
									parser2.reset(reader, theGit.getRepository().resolve(merged.name() + "^{tree}"));
									CommitImpl commit = parseCommit(merged, parser1, parser2, false);
									previous = merged;
									CanonicalTreeParser tempTree = parser1;
									parser1 = parser2;
									parser2 = tempTree;
									fireListeners(commit);
								}
							}
						}
						break;
					case ALREADY_UP_TO_DATE:
						break;
					}
					break;
				case NEW:
					throw new IOException("Don't know how to handle " + update.getResult());
				case NO_CHANGE:
					break;
				}
			}
			// Pull successful
		}

		// Push if we have anything new.
		Ref head = theGit.getRepository().getRefDatabase().findRef(Constants.HEAD);
		if (status == null || status.getAheadCount() > 0) {
			try {
				theGit.push()//
					.add(head)//
					.setRemote(remoteName)//
					.setRefSpecs(new RefSpec(shortBranch + ":" + shortBranch))//
					.setCredentialsProvider(theCredentials).call();
			} catch (GitAPIException e) {
				throw new IOException("Push failed", e);
			}
		}
		return this;
	}

	static class CommitTree {
		final RevCommit commit;
		final CommitTree child;
		final int depth;
		CommitTree[] parents;

		CommitTree(RevCommit commit, CommitTree child) {
			this.commit = commit;
			this.child = child;
			depth = child == null ? 1 : child.depth + 1;
		}

		List<RevCommit> getPath() {
			List<RevCommit> path = new ArrayList<>(depth);
			CommitTree tree = this;
			while (tree != null) {
				path.add(tree.commit);
				tree = tree.child;
			}
			return path;
		}
	}

	private static List<RevCommit> findPath(RevCommit oldHead, RevCommit newHead) {
		CircularArrayList<CommitTree> trees = CircularArrayList.build().build();
		trees.add(new CommitTree(newHead, null));
		while (true) {
			CommitTree tree = trees.removeFirst();
			for (int i = 0; i < tree.commit.getParentCount(); i++) {
				RevCommit parent = tree.commit.getParent(i);
				if (parent.equals(oldHead)) {
					return tree.getPath();
				}
				trees.add(new CommitTree(parent, tree));
			}
		}
	}

	private EntityFormat getEntityForFile(String file, boolean added, File content) throws IOException {
		if (!file.endsWith(".csv")) {
			return null;
		}
		int lastSlash = file.lastIndexOf('/');
		if (lastSlash < 0) {
			return null;
		}
		int penultimateSlash = file.lastIndexOf('/', lastSlash - 1);
		String entityName = file.substring(penultimateSlash < 0 ? 0 : penultimateSlash + 1, lastSlash);
		String fileName = file.substring(lastSlash + 1, file.length() - 4);
		if (!fileName.startsWith(entityName) || fileName.charAt(entityName.length()) != '_') {
			return null;
		}
		EntityFormat entity = getEntityType(entityName);
		if (entity != null) {
			return entity;
		} else if (added) {
			// Maybe a new entity type?
			try (Reader reader = new BufferedReader(new FileReader(content))) {
				CsvParser parser = new CsvParser(reader, ',');
				try {
					String[] header = parser.parseNextLine();
					EntityFormat format = parseHeader(entityName, header, parser);
					// Can only change the schema through the API on a fresh branch, but if it's coming down from on high,
					// we can't enforce that.
					boolean preFresh = isFreshBranch;
					isFreshBranch = true;
					format = addEntityType(entityName, format.getFields().asJavaMap(),
						format.getFieldOrder().subList(0, format.getIdFieldCount()));
					isFreshBranch = preFresh;
					return format;
				} catch (TextParseException e) {
					System.err.println("Bad header for potentiall new entity file " + file);
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	private CommitImpl parseCommit(RevCommit commit, CanonicalTreeParser parentTree, CanonicalTreeParser tree, boolean localOnly)
		throws IOException {
		CommitterImpl committer = new CommitterImpl(commit.getAuthorIdent().getName());
		CommitImpl commitImpl = new CommitImpl(committer, commit.getAuthorIdent().getWhen().toInstant(), commit.getFullMessage(),
			localOnly);
		byte[] buffer = new byte[5 * 1024];
		class Diff {
			final DiffEntry entry;
			final String entityName;
			final int fileIndex;

			Diff(DiffEntry entry, String entityName, int fileIndex) {
				this.entry = entry;
				this.entityName = entityName;
				this.fileIndex = fileIndex;
			}
		}
		List<Diff> diffs = new ArrayList<>();
		try (DiffFormatter formatter = new DiffFormatter(DisabledOutputStream.INSTANCE)) {
			formatter.setRepository(theGit.getRepository());
			formatter.setDiffComparator(RawTextComparator.DEFAULT);
			for (DiffEntry diff : formatter.scan(parentTree, tree)) {
				String file = diff.getNewPath();
				if (file == null) {
					file=diff.getOldPath();
				}
				if (!file.endsWith(".csv")) {
					continue;
				}
				int lastSlash = file.lastIndexOf('/');
				if (lastSlash < 0) {
					continue;
				}
				int penultimateSlash = file.lastIndexOf('/', lastSlash - 1);
				String entityName = file.substring(penultimateSlash < 0 ? 0 : penultimateSlash + 1, lastSlash);
				String fileName = file.substring(lastSlash + 1, file.length() - 4);
				if (!fileName.startsWith(entityName) || fileName.charAt(entityName.length()) != '_') {
					continue;
				}
				int fileIndex;
				try {
					fileIndex = Integer.parseInt(file.substring(lastSlash + 1 + entityName.length() + 1, file.length() - 4));
				} catch (NumberFormatException e) {
					continue;
				}
				diffs.add(new Diff(diff, entityName, fileIndex));
			}
		}
		for (Diff diff : diffs) {
			String file = diff.entry.getNewPath();
			if (file == null) {
				file=diff.entry.getOldPath();
			}
			File tempNewFile = null;
			if (diff.entry.getChangeType() == ChangeType.ADD || diff.entry.getChangeType() == ChangeType.MODIFY) {
				tempNewFile = File.createTempFile(diff.entityName + "_" + diff.fileIndex, ".new.csv");
				try (TreeWalk treeWalk = new TreeWalk(theGit.getRepository())) {
					tree.reset();
					treeWalk.addTree(tree);
					treeWalk.setRecursive(true);
					treeWalk.setFilter(PathFilter.create(file));
					if (!treeWalk.next()) {
						throw new IllegalStateException(file + " not found");
					}
					ObjectId object = treeWalk.getObjectId(0);
					ObjectLoader loader = theGit.getRepository().open(object);
					try (OutputStream out = new FileOutputStream(tempNewFile)) {
						loader.copyTo(out);
					}
				}
			}
			EntityFormat entity = getEntityForFile(file, diff.entry.getChangeType() == ChangeType.ADD, tempNewFile);
			if (entity == null) {
				continue; // Don't care
			}
			File tempOldFile = null;
			if (diff.entry.getChangeType() == ChangeType.DELETE || diff.entry.getChangeType() == ChangeType.MODIFY) {
				tempOldFile = File.createTempFile(diff.entityName + "_" + diff.fileIndex, ".old.csv");
				try (TreeWalk treeWalk = new TreeWalk(theGit.getRepository())) {
					parentTree.reset();
					treeWalk.addTree(parentTree);
					treeWalk.setRecursive(true);
					treeWalk.setFilter(PathFilter.create(file));
					treeWalk.next();
					ObjectId object = treeWalk.getObjectId(0);
					ObjectLoader loader = theGit.getRepository().open(object);
					try (OutputStream out = new FileOutputStream(tempOldFile)) {
						loader.copyTo(out);
					}
				}
			}
			switch (diff.entry.getChangeType()) {
			case COPY:
			case RENAME:
				break; // These don't affect the content
			case ADD:
				try (EntityGetterImpl getter = iterate(entity, tempNewFile, diff.fileIndex)) {
					for (QuickMap<String, Object> added = getter.getNextGoodId(); added != null; added = getter.getNextGoodId()) {
						try {
							commitImpl.addChange(entity, null, getter.getLast(), diff.fileIndex);
						} catch (TextParseException e) {}
					}
				}
				break;
			case DELETE:
				try (EntityGetterImpl getter = iterate(entity, tempOldFile, diff.fileIndex)) {
					for (QuickMap<String, Object> removed = getter.getNextGoodId(); removed != null; removed = getter.getNextGoodId()) {
						try {
							commitImpl.addChange(entity, removed, getter.getLast(), diff.fileIndex);
						} catch (TextParseException e) {}
					}
				}
				break;
			case MODIFY:
				try {
					parseEdit(entity, file, tempOldFile, tempNewFile, commitImpl, buffer, diff.fileIndex);
				} catch (IOException e) {
					System.err.println("Could not parse edit");
					e.printStackTrace();
				}
				break;
			}
			if (tempNewFile != null) {
				tempNewFile.delete();
			}
			if (tempOldFile != null) {
				tempOldFile.delete();
			}
		}
		return commitImpl;
	}

	private void parseEdit(EntityFormat entity, String file, File oldFile, File newFile, CommitImpl commit, byte[] buffer, int fileIndex)
		throws IOException {
		try (Reader oldReader = new BufferedReader(new InputStreamReader(new FileInputStream(oldFile), UTF8));
			Reader newReader = new BufferedReader(new InputStreamReader(new FileInputStream(newFile), UTF8))) {
			CsvParser oldParser = new CsvParser(oldReader, ',');
			CsvParser newParser = new CsvParser(newReader, ',');
			// Don't diff against headers
			try {
				oldParser.parseNextLine();
			} catch (TextParseException e) {
				throw new IOException(file + " old version could not be parsed as CSV", e);
			}
			try {
				newParser.parseNextLine();
			} catch (TextParseException e) {
				throw new IOException(file + " new version could not be parsed as CSV", e);
			}
			class Entry {
				final CsvParser parser;
				final boolean isNew;
				final String[] line = new String[entity.getFields().keySize()];
				final QuickMap<String, Object> fields = entity.create(false);
				boolean exists;
				boolean hasIds;
				boolean isFull;

				Entry(CsvParser parser, boolean isNew) {
					this.parser = parser;
					this.isNew = isNew;
				}

				Entry readLine() throws IOException, TextParseException {
					exists = parser.parseNextLine(line);
					hasIds = isFull = false;
					return this;
				}

				Entry fillIds() throws IOException, TextParseException {
					if (hasIds) {
						return this;
					}
					parseIds(entity, line, fields, parser, false);
					return this;
				}

				boolean fillNonIds() throws IOException {
					if (isFull) {
						return true;
					}
					try {
						parseNonIds(entity, line, fields, parser);
					} catch (TextParseException e) {
						System.err.println("Could not parse " + (isNew ? "new" : "old") + " entity values: " + entity.getName() + " "
							+ Arrays.toString(line));
						e.printStackTrace();
						return false;
					}
					isFull = true;
					return true;
				}
			}
			try {
				Entry oldEntry = new Entry(oldParser, false).readLine(), newEntry = new Entry(newParser, false).readLine();
				while (oldEntry.exists || newEntry.exists) {
					if (Arrays.equals(oldEntry.line, newEntry.line)) {
						oldEntry.readLine();
						newEntry.readLine();
					} else if (!oldEntry.exists) {
						if (newEntry.fillIds().fillNonIds()) {
							commit.addChange(entity, null, newEntry.fields.copy().unmodifiable(), fileIndex);
						}
						newEntry.readLine();
					} else if (!newEntry.exists) {
						if (oldEntry.fillIds().fillNonIds()) {
							commit.addChange(entity, oldEntry.fields.copy().unmodifiable(), null, fileIndex);
						}
						oldEntry.readLine();
					} else {
						boolean sameId = true;
						for (int i = 0; sameId && i < entity.getIdFieldCount(); i++) {
							if (!oldEntry.line[i].equals(newEntry.line[i])) {
								sameId = false;
							}
						}
						int entityComp = 0;
						if (!sameId) {
							entityComp = entity.compareIds(oldEntry.fillIds().fields, newEntry.fillIds().fields);
							sameId = entityComp == 0;
						}
						if (sameId) {
							if (oldEntry.fillIds().fillNonIds() && newEntry.fillIds().fillNonIds()) {
								commit.addChange(entity, oldEntry.fields.copy().unmodifiable(), newEntry.fields.copy().unmodifiable(),
									fileIndex);
							}
							oldEntry.readLine();
							newEntry.readLine();
						} else {
							if (entityComp < 0) { // Deleted old entry
								if (oldEntry.fillIds().fillNonIds()) {
									commit.addChange(entity, oldEntry.fields.copy().unmodifiable(), null, fileIndex);
								}
								oldEntry.readLine();
							} else { // Added new entry
								if (newEntry.fillIds().fillNonIds()) {
									commit.addChange(entity, null, newEntry.fields.copy().unmodifiable(), fileIndex);
								}
								newEntry.readLine();
							}
						}
					}
				}
			} catch (TextParseException e) {
				throw new IOException("Could not diff old and new versions of " + file, e);
			}
		}
	}

	static class RAFIS extends InputStream {
		private final RandomAccessFile theRAF;

		RAFIS(RandomAccessFile rAF) {
			theRAF = rAF;
		}

		@Override
		public int read() throws IOException {
			return theRAF.read();
		}

		@Override
		public int read(byte[] b) throws IOException {
			return theRAF.read(b);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return theRAF.read(b, off, len);
		}
	}

	private void fireListeners(Commit commit) {
		theListeners.forEach(//
			listener -> {
				if (commit.isLocalOnly() && listener.remoteOnly) {
					return;
				}
				listener.listener.changeOccurred(commit);
			});
	}

	@Override
	public VersionedEntities commit(String message) throws IOException {
		if (!hasModifications) {
			return this;
		}
		try {
			theGit.commit()//
				.setMessage(message == null ? autoGenMessage() : message)//
				.setAll(true)//
				.call();
		} catch (GitAPIException e) {
			throw new IOException("Could not commit changes", e);
		}
		if (theLocalListeners > 0) {
			// TODO
		}
		isFreshBranch = getEntityTypes().isEmpty();
		hasModifications = false;
		return this;
	}

	/**
	 * @return The commit message for a new commit if none is specified
	 * @throws IOException If an error occurs accessing the information needed to generate the message
	 */
	protected String autoGenMessage() throws IOException {
		if (isFreshBranch) {
			return getClass().getSimpleName() + " Initial branch commit: " + theGit.getRepository().getBranch();
		} else {
			return getClass().getSimpleName() + " auto-generated commit ";
		}
	}

	@Override
	public BranchInfo getBranch() throws IOException {
		return BranchInfo.parseBranch(theGit.getRepository().getBranch());
	}

	@Override
	public VersionedEntities branch(String newBranchName, int changeLevel) throws IOException {
		BranchInfo currentBranch = getBranch();
		int[] version = new int[] { currentBranch.getMajorVersion(), currentBranch.getMinorVersion(), currentBranch.getPatchVersion() };
		switch (changeLevel) {
		case 0:
			break;
		case 1:
			version[0]++;
			version[1] = version[2] = 0;
			break;
		case 2:
			version[1]++;
			version[2] = 0;
			break;
		case 3:
			version[2]++;
			break;
		default:
			throw new IllegalArgumentException("Unrecognized change level: " + changeLevel + ". Expected 0, 1, 2, or 3");
		}
		BranchInfo newBranch = new BranchInfo(//
			newBranchName == null ? currentBranch.getBranchName() : newBranchName, //
			version[0], version[1], version[2]);
		// TODO We should probably check with the remote to see if a branch with this name has already been created
		// If so, we should throw an exception--schema changes shouldn't be coming from multiple sources at the same time
		try {
			theGit.branchCreate().setName(newBranchName.toString()).setUpstreamMode(SetupUpstreamMode.TRACK).call();
		} catch (GitAPIException e) {
			throw new IOException("Could not create branch " + newBranch, e);
		}
		return this;
	}

	@Override
	protected void schemaChanged() {
		if (!isFreshBranch) {
			throw new IllegalStateException("The version must be branched immediately before schema change.  Use branch(String, int)");
		}
		hasModifications = true;
		super.schemaChanged();
	}

	@Override
	protected void fileAdded(File file) throws IOException {
		try {
			theGit.add().addFilepattern(getFilePattern(file)).call();
		} catch (GitAPIException e) {
			throw new IOException("Unable to add file", e);
		}
		hasModifications = true;
		super.fileAdded(file);
	}

	@Override
	protected void fileRemoved(File file) throws IOException {
		try {
			theGit.rm().addFilepattern(getFilePattern(file)).call();
		} catch (GitAPIException e) {
			throw new IOException("Unable to remove file", e);
		}
		hasModifications = true;
		super.fileRemoved(file);
	}

	@Override
	protected void fileChanged(File file) throws IOException {
		try {
			theGit.add().setUpdate(true).addFilepattern(getFilePattern(file)).call();
		} catch (GitAPIException e) {
			throw new IOException("Unable to add file", e);
		}
		hasModifications = true;
		super.fileChanged(file);
	}

	@Override
	protected void fileRenamed(File oldFile, File newFile) throws IOException {
		try {
			theGit.rm().addFilepattern(getFilePattern(oldFile)).call();
			theGit.add().addFilepattern(getFilePattern(newFile)).call();
		} catch (GitAPIException e) {
			throw new IOException("Unable to rename file", e);
		}
		hasModifications = true;
		super.fileRenamed(oldFile, newFile);
	}

	private String getFilePattern(File file) {
		StringBuilder str = new StringBuilder();
		File f = file;
		while (f != null && !f.equals(theRepoRoot)) {
			if (str.length() > 0) {
				str.append('/');
			}
			str.append(new ReversedSequence(f.getName()));
			f = f.getParentFile();
		}
		if (f == null) {
			throw new IllegalStateException("File " + file.getPath() + " does not exist inside repository " + theRepoRoot.getPath());
		}
		return new ReversedSequence(str).toString();
	}

	private static class ReversedSequence implements CharSequence {
		private final CharSequence theSequence;

		ReversedSequence(CharSequence sequence) {
			theSequence = sequence;
		}

		@Override
		public int length() {
			return theSequence.length();
		}

		@Override
		public char charAt(int index) {
			return theSequence.charAt(theSequence.length() - 1 - index);
		}

		@Override
		public CharSequence subSequence(int start, int end) {
			int len = theSequence.length();
			return new ReversedSequence(theSequence.subSequence(len - end - 1, len - start - 1));
		}

		@Override
		public String toString() {
			char[] chars = new char[theSequence.length()];
			for (int i = 0; i < chars.length; i++) {
				chars[i] = theSequence.charAt(chars.length - i - 1);
			}
			return new String(chars);
		}
	}

	private static class CommitterImpl implements Committer {
		private final String theName;

		CommitterImpl(String name) {
			theName = name;
		}

		@Override
		public String getName() {
			return theName;
		}
	}

	private static class CommitImpl implements Commit {
		private final CommitterImpl theCommitter;
		private final Instant theCommitTime;
		private final String theMessage;
		private final boolean isLocalOnly;
		private final List<EntityUpdate> theChanges;

		CommitImpl(CommitterImpl committer, Instant commitTime, String message, boolean localOnly) {
			theCommitter = committer;
			theCommitTime = commitTime;
			theMessage = message;
			isLocalOnly = localOnly;
			theChanges = new ArrayList<>();
		}

		void addChange(EntityFormat entityType, QuickMap<String, Object> oldValues, QuickMap<String, Object> newValues, int fileIndex) {
			theChanges.add(new EntityUpdateImpl(this, entityType, oldValues, newValues, fileIndex));
		}

		@Override
		public Committer getCommitter() {
			return theCommitter;
		}

		@Override
		public Instant getCommitTime() {
			return theCommitTime;
		}

		@Override
		public String getMessage() {
			return theMessage;
		}

		@Override
		public boolean isLocalOnly() {
			return isLocalOnly;
		}

		@Override
		public List<EntityUpdate> getChanges() {
			return Collections.unmodifiableList(theChanges);
		}
	}

	static class EntityUpdateImpl implements EntityUpdate {
		private final CommitImpl theCommit;
		private final EntityFormat theEntityType;
		private final QuickMap<String, Object> theOldValues;
		private final QuickMap<String, Object> theNewValues;
		private final int theFileIndex;

		EntityUpdateImpl(CommitImpl commit, EntityFormat entityType, QuickMap<String, Object> oldValues, QuickMap<String, Object> newValues,
			int fileIndex) {
			theCommit = commit;
			theEntityType = entityType;
			theOldValues = oldValues;
			theNewValues = newValues;
			theFileIndex = fileIndex;
		}

		@Override
		public Commit getCommit() {
			return theCommit;
		}

		@Override
		public EntityFormat getEntityType() {
			return theEntityType;
		}

		@Override
		public QuickMap<String, Object> getOldValues() {
			return theOldValues;
		}

		@Override
		public QuickMap<String, Object> getNewValues() {
			return theNewValues;
		}

		public int getFileIndex() {
			return theFileIndex;
		}
	}
}
