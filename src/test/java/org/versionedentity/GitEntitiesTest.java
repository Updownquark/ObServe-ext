package org.versionedentity;

import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.Assert;
import org.junit.Test;
import org.observe.util.CsvEntitySetTest;
import org.observe.util.VersionedEntities.ChangeListener;
import org.observe.util.VersionedEntities.Commit;
import org.observe.util.VersionedEntities.EntityUpdate;
import org.qommons.collect.BetterSortedMap;
import org.qommons.collect.CircularArrayList;
import org.qommons.collect.QuickSet.QuickMap;
import org.qommons.io.FileUtils;
import org.qommons.io.TextParseException;
import org.qommons.tree.BetterTreeList;
import org.qommons.tree.BetterTreeMap;

/** Tests GitEntities */
public class GitEntitiesTest {
	/**
	 * Ensures the basic CRUD functionality of {@link GitEntities} works
	 * 
	 * @throws GitAPIException If an error occurs in the Git API
	 * @throws IOException If an error occurs reading or writing the data
	 * @throws TextParseException If an error occurs parsing entity data
	 */
	@SuppressWarnings("static-method")
	@Test
	public void testBasic() throws GitAPIException, IOException, TextParseException {
		File testDir = new File(System.getProperty("user.home") + "/" + GitEntitiesTest.class.getSimpleName());
		if (testDir.exists()) {
			FileUtils.delete(testDir, null);
		}

		// Set up 3 repositories--a master to serve as the remote, and 2 subordinate repos
		System.out.println("Initializing master repository");
		Git master = Git.init()//
			.setDirectory(new File(testDir, "master"))//
			.call();
		master.commit()// Need a commit to create the master branch
			.setAllowEmpty(true)//
			.setMessage("Initial commit")//
			.call();
		System.out.println("Cloning copy1 repository");
		Git copy1 = Git.cloneRepository()//
			.setURI(master.getRepository().getDirectory().getParentFile().toURI().toString())//
			.setDirectory(new File(testDir, "copy1"))//
			.call();
		System.out.println("Cloning copy2 repository");
		Git copy2 = Git.cloneRepository()//
			.setURI(master.getRepository().getDirectory().getParentFile().toURI().toString())//
			.setDirectory(new File(testDir, "copy2"))//
			.call();

		// Set up a GitEntitySet for one subordinate, initialize, populate, and push it
		File indexes = new File(testDir, "indexes");
		GitEntities entities1 = null, entities2 = null;
		try {
			System.out.println("Setting up entity set 1");
			entities1 = new GitEntities(copy1, new File(indexes, "copy1"), null, null);
			Deque<EntityUpdate> changes1 = CircularArrayList.build().build();
			entities1.addListener(new ChangeListener() {
				@Override
				public void changeOccurred(Commit commit) {
					changes1.addAll(commit.getChanges());
				}
			}, true);
			BetterSortedMap<Long, QuickMap<String, Object>> existing1 = BetterTreeMap.build(Long::compareTo).buildMap();
			CsvEntitySetTest.initSimpleEntitySet(entities1);
			System.out.println("Populating initial entities");
			for (int i = 0; i < 20; i++) {
				QuickMap<String, Object> entity = CsvEntitySetTest.addTestEntity(entities1, i);
				existing1.put((Long) entity.get("id"), entity);
			}
			System.out.println("Pushing initial entities to master");
			entities1.commit("Initial entities");
			entities1.checkAndPush(null);

			// Set up a GitEntitySet for the other sub, pull, and verify its content against the other sub.
			System.out.println("Setting up entity set 2");
			entities2 = new GitEntities(copy2, new File(indexes, "copy2"), null, null);
			Deque<EntityUpdate> changes2 = CircularArrayList.build().build();
			entities2.addListener(new ChangeListener() {
				@Override
				public void changeOccurred(Commit commit) {
					changes2.addAll(commit.getChanges());
				}
			}, true);
			BetterSortedMap<Long, QuickMap<String, Object>> existing2 = BetterTreeMap.build(Long::compareTo).buildMap();
			Assert.assertTrue(entities2.getEntityTypes().isEmpty());
			System.out.println("Pulling initial entities into entity set 2");
			entities2.checkAndPush(null);
			Assert.assertNotNull(entities2.getEntityType("test1"));
			Assert.assertEquals(entities1.getEntityType("test1").getFields(), entities2.getEntityType("test1").getFields());
			Assert.assertEquals(existing1.size(), changes2.size());
			for (EntityUpdate change = changes2.pollFirst(); change != null; change = changes2.pollFirst()) {
				Assert.assertNull(change.getOldValues());
				Assert.assertEquals(existing1.get(change.getNewValues().get("id")), change.getNewValues());
				existing2.put((Long) change.getNewValues().get("id"), change.getNewValues().copy());
				Assert.assertEquals(change.getNewValues(), entities2.get("test1", change.getNewValues()));
			}

			// Make some changes to the second entity set
			System.out.println("Initial entity modifications");
			long added = existing2.keySet().last() + 1;
			QuickMap<String, Object> entity = entities2.getEntityType("test1").create(false)//
				.with("id", added)//
				.with("name", "Entity " + added)//
				.with("values", BetterTreeList.build().build()//
					.with(0, 3, 6, 9, 12, 15));
			existing2.put(added, entity);
			Assert.assertFalse(entities2.update("test1", entity, true));
			long removed = existing2.keySet().get(existing2.size() / 2);
			entity = existing2.remove(removed);
			Assert.assertTrue(entities2.delete("test1", entity));
			long updated = existing2.keySet().get(4);
			List<Integer> oldValues = (List<Integer>) existing2.get(updated).get("values");
			Assert.assertTrue(entities2.update("test1", existing2.get(updated).with("name", "Entity " + updated + "B"), false));
			// Push to the master, pull from the other copy
			System.out.println("Pushing entity modifications");
			entities2.commit("First entity modifications");
			entities2.checkAndPush(null);
			System.out.println("Pulling entity modifications");
			entities1.checkAndPush(null);
			// Ensure the changes "took" on the other copy
			Assert.assertEquals(3, changes1.size());
			for (EntityUpdate change = changes1.pollFirst(); change != null; change = changes1.pollFirst()) {
				if (change.getOldValues() == null) { // The addition
					Assert.assertEquals(existing2.get(added), change.getNewValues());
					Assert.assertEquals(change.getNewValues(), entities1.get("test1", change.getNewValues()));
					existing1.put(added, change.getNewValues().copy());
				} else if (change.getNewValues() == null) { // The removal
					Assert.assertEquals(entity, change.getOldValues());
					Assert.assertNull(entities1.get("test1", change.getOldValues()));
				} else { // The update
					Assert.assertEquals(oldValues, change.getOldValues().get("values"));
					Assert.assertEquals(existing2.get(updated), change.getNewValues());
					Assert.assertEquals(change.getNewValues(), entities1.get("test1", change.getOldValues()));
				}
			}

			// TODO
			// Randomly perform a small set of operations on one of the subs, push, then pull the other sub and verify the changes "took"
			// and that listeners were fired correctly
			// Occasionally perform small operation sets on *both* subs, push from one, pull the other, verifying appropriate merge
			// and conflict-resolution behavior, change effects, and event firing, then push it and pull the first sub, verifying the
			// merged changes "took" and listeners fired correctly
		} finally {
			if (entities1 != null) {
				entities1.close();
			}
			if (entities2 != null) {
				entities2.close();
			}
			FileUtils.delete(testDir, null);
		}
	}
}
