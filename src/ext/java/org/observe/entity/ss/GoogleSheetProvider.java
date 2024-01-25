package org.observe.entity.ss;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.observe.config.OperationResult;
import org.observe.entity.ConfigurableOperation;
import org.observe.entity.EntityChange;
import org.observe.entity.EntityCreator;
import org.observe.entity.EntityDeletion;
import org.observe.entity.EntityLoadRequest;
import org.observe.entity.EntityLoadRequest.Fulfillment;
import org.observe.entity.EntityOperationException;
import org.observe.entity.EntityQuery;
import org.observe.entity.EntityUpdate;
import org.observe.entity.ObservableEntityDataSet;
import org.observe.entity.ObservableEntityFieldType;
import org.observe.entity.ObservableEntityProvider;
import org.observe.entity.ObservableEntityType;
import org.qommons.StringUtils;
import org.qommons.collect.BetterCollection;
import org.qommons.collect.ElementId;
import org.qommons.collect.MultiMap;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.BatchUpdateValuesRequest;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.DataFilter;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GetSpreadsheetByDataFilterRequest;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.ValueRange;

public class GoogleSheetProvider implements ObservableEntityProvider {
	private static class SheetInfo {
		final int sheetId;
		final String sheetName;
		final String[] columns;

		SheetInfo(int sheetId, String sheetName, String[] columns) {
			this.sheetId = sheetId;
			this.sheetName = sheetName;
			this.columns = columns;
		}
	}

	private final Sheets theService;
	private String theSpreadSheetId;
	private final List<SheetInfo> theSheetContent;
	private boolean printSheetIdOnCreate;

	public GoogleSheetProvider(Sheets service, String spreadSheetId) {
		theService = service;
		theSpreadSheetId = spreadSheetId;
		theSheetContent = new ArrayList<>();
	}

	public String getSpreadSheetId() {
		return theSpreadSheetId;
	}

	public GoogleSheetProvider setPrintSheetIdOnCreate(boolean print) {
		printSheetIdOnCreate = print;
		return this;
	}

	@Override
	public void install(ObservableEntityDataSet entitySet) throws EntityOperationException {
		if (theSpreadSheetId != null) {
			try {
				List<SheetProperties> sheets = new ArrayList<>();
				Spreadsheet ss = theService.spreadsheets().get(theSpreadSheetId).execute();
				for (Sheet sheet : ss.getSheets()) {
					sheets.add(sheet.getProperties());
				}
				List<DataFilter> filters = new ArrayList<>(sheets.size());
				for (SheetProperties sheet : sheets) {
					filters.add(new DataFilter().setGridRange(new GridRange()//
							.setSheetId(sheet.getSheetId()).setStartRowIndex(0).setStartColumnIndex(0)//
							.setEndColumnIndex(sheet.getGridProperties().getColumnCount())));
				}
				String[][] headers = theService.spreadsheets()
						.getByDataFilter(theSpreadSheetId, //
								new GetSpreadsheetByDataFilterRequest().setDataFilters(filters))
						.execute().getSheets().stream().flatMap(sheet -> sheet.getData().stream())//
						.map(data -> data.getRowData().get(0).getValues().stream().map(cell -> cell.getFormattedValue())
								.toArray(String[]::new))
						.toArray(String[][]::new);
				for (int s = 0; s < sheets.size(); s++) {
					theSheetContent.add(new SheetInfo(sheets.get(s).getSheetId(), sheets.get(s).getTitle(), headers[s]));
				}
			} catch (IOException e) {
				throw new EntityOperationException("Spreadsheet retrieval call failed", e);
			}

			// TODO Synchronize schema
		} else {
			SpreadsheetProperties props = new SpreadsheetProperties();
			props.setTitle(theService.getApplicationName());
			Spreadsheet newSheet = new Spreadsheet();
			newSheet.setProperties(props);
			List<Sheet> sheets = new ArrayList<>();
			BatchUpdateValuesRequest headerSet = new BatchUpdateValuesRequest().setData(new ArrayList<>());
			headerSet.setValueInputOption("RAW");
			for (ObservableEntityType<?> entity : entitySet.getEntityTypes()) {
				Sheet sheet = new Sheet();
				sheets.add(sheet);
				List<CellData> headerCells = new ArrayList<>(entity.getFields().keySize());
				for (ObservableEntityFieldType<?, ?> field : entity.getFields().allValues()) {
					headerCells.add(new CellData().setEffectiveValue(new ExtendedValue().setStringValue(//
							StringUtils.parseByCase(field.getName(), true).toCaseScheme(true, true, " "))));
				}
				sheet.setProperties(new SheetProperties()//
						.setTitle(StringUtils.parseByCase(entity.getName(), true).toCaseScheme(true, true, " ")));

				headerSet.getData()
						.add(new ValueRange()//
								.setRange(getRange(sheet.getProperties().getTitle(), 0, 0, entity.getFields().keySize(), 0))//
								.setValues(Arrays.asList(entity.getFields().keySet().stream()//
										.map(field -> StringUtils.parseByCase(field, true).toCaseScheme(true, true, " "))
										.collect(Collectors.toList()))));
			}
			newSheet.setSheets(sheets);
			try {
				theSpreadSheetId = theService.spreadsheets().create(newSheet).execute().getSpreadsheetId();
			} catch (IOException e) {
				throw new EntityOperationException("Spreadsheet creation call failed", e);
			}
			if (printSheetIdOnCreate) {
				System.out.println("Created sheet "+theSpreadSheetId+" ("+props.getTitle()+")");
			}
			try {
				theService.spreadsheets().values().batchUpdate(theSpreadSheetId, headerSet).execute();
			} catch (IOException e) {
				throw new EntityOperationException("Spreadsheet header initialization call failed", e);
			}
		}
	}

	private static String getRange(String sheetTitle, int startCol, int startRow, int endCol, int endRow) {
		StringBuilder range = new StringBuilder();
		range.append(sheetTitle).append('!');
		appendColumn(range, startCol);
		range.append(startRow + 1);
		range.append(':');
		appendColumn(range, endCol);
		range.append(endRow + 1);
		return range.toString();
	}

	private static void appendColumn(StringBuilder range, int startCol) {
		do {
			int mod = startCol % 26;
			startCol /= 26;
			range.append((char) ('A' + mod));
		} while (startCol > 0);
	}

	@Override
	public Object prepare(ConfigurableOperation<?> operation) throws EntityOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void dispose(Object prepared) {
		// TODO Auto-generated method stub

	}

	@Override
	public <E> SimpleEntity<E> create(EntityCreator<? super E, E> creator, Object prepared, boolean reportInChanges)
			throws EntityOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <E> OperationResult<SimpleEntity<E>> createAsync(EntityCreator<? super E, E> creator, Object prepared, boolean reportInChanges) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationResult<Long> count(EntityQuery<?> query, Object prepared) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <E> OperationResult<Iterable<SimpleEntity<? extends E>>> query(EntityQuery<E> query, Object prepared) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <E> long update(EntityUpdate<E> update, Object prepared, boolean reportInChanges) throws EntityOperationException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <E> OperationResult<Long> updateAsync(EntityUpdate<E> update, Object prepared, boolean reportInChanges) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <E> long delete(EntityDeletion<E> delete, Object prepared, boolean reportInChanges) throws EntityOperationException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <E> OperationResult<Long> deleteAsync(EntityDeletion<E> delete, Object prepared, boolean reportInChanges) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V> ElementId updateCollection(BetterCollection<V> collection, CollectionOperationType changeType, ElementId element, V value,
			boolean reportInChanges) throws EntityOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <V> OperationResult<ElementId> updateCollectionAsync(BetterCollection<V> collection, CollectionOperationType changeType,
			ElementId element, V value, boolean reportInChanges) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> ElementId updateMap(Map<K, V> collection, CollectionOperationType changeType, K key, V value, Runnable asyncResult) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K, V> ElementId updateMultiMap(MultiMap<K, V> collection, CollectionOperationType changeType, ElementId valueElement, K key,
			V value, Consumer<ElementId> asyncResult) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<EntityChange<?>> changes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Fulfillment<?>> loadEntityData(List<EntityLoadRequest<?>> loadRequests) throws EntityOperationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationResult<List<Fulfillment<?>>> loadEntityDataAsync(List<EntityLoadRequest<?>> loadRequests) {
		// TODO Auto-generated method stub
		return null;
	}
}
