package org.versionedentity;

import java.io.IOException;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.FetchResult;
import org.eclipse.jgit.transport.TrackingRefUpdate;

public class GitEntities extends VersionedEntities {
	private final Git theGit;

	@Override
	public GitEntities checkAndPush() throws IOException {
		FetchResult fetch;
		try {
			fetch = theGit.fetch().call();
		} catch (GitAPIException e) {
			throw new IOException("Unable to fetch", e);
		}
		TrackingRefUpdate update = fetch.getTrackingRefUpdate(theGit.getRepository().getFullBranch());
		// if(update
	}
}
