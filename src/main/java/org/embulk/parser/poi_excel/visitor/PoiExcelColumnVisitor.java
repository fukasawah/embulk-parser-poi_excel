package org.embulk.parser.poi_excel.visitor;

import java.text.MessageFormat;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellReference;
import org.embulk.parser.poi_excel.PoiExcelColumnValueType;
import org.embulk.parser.poi_excel.PoiExcelParserPlugin.ColumnOptionTask;
import org.embulk.parser.poi_excel.PoiExcelParserPlugin.PluginTask;
import org.embulk.parser.poi_excel.visitor.embulk.CellVisitor;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;

import com.google.common.base.Optional;

public class PoiExcelColumnVisitor implements ColumnVisitor {

	protected final PoiExcelVisitorValue visitorValue;
	protected final PageBuilder pageBuilder;
	protected final PoiExcelVisitorFactory factory;

	protected Row currentRow;

	public PoiExcelColumnVisitor(PoiExcelVisitorValue visitorValue) {
		this.visitorValue = visitorValue;
		this.pageBuilder = visitorValue.getPageBuilder();
		this.factory = visitorValue.getVisitorFactory();

		initializeColumnOptions();
	}

	protected void initializeColumnOptions() {
		PluginTask task = visitorValue.getPluginTask();
		List<ColumnOptionTask> options = visitorValue.getColumnOptions();
		new PoiExcelColumnIndex().initializeColumnIndex(task, options);
	}

	public void setRow(Row row) {
		this.currentRow = row;
	}

	@Override
	public final void booleanColumn(Column column) {
		visitCell0(column, factory.getBooleanCellVisitor());
	}

	@Override
	public final void longColumn(Column column) {
		visitCell0(column, factory.getLongCellVisitor());
	}

	@Override
	public final void doubleColumn(Column column) {
		visitCell0(column, factory.getDoubleCellVisitor());
	}

	@Override
	public final void stringColumn(Column column) {
		visitCell0(column, factory.getStringCellVisitor());
	}

	@Override
	public final void timestampColumn(Column column) {
		visitCell0(column, factory.getTimestampCellVisitor());
	}

	protected final void visitCell0(Column column, CellVisitor visitor) {
		try {
			visitCell(column, visitor);
		} catch (Exception e) {
			String sheetName = visitorValue.getSheet().getSheetName();
			String ref = new CellReference(currentRow.getRowNum(), visitorValue.getColumnOption(column)
					.getColumnIndex()).formatAsString();
			throw new RuntimeException(MessageFormat.format("error at {0} cell={1}!{2}. {3}", column, sheetName, ref,
					e.getMessage()), e);
		}
	}

	protected void visitCell(Column column, CellVisitor visitor) {
		ColumnOptionTask option = visitorValue.getColumnOption(column);
		PoiExcelColumnValueType valueType = option.getValueTypeEnum();

		switch (valueType) {
		case SHEET_NAME:
			visitor.visitSheetName(column);
			return;
		case ROW_NUMBER:
			visitor.visitRowNumber(column, currentRow.getRowNum() + 1);
			return;
		case COLUMN_NUMBER:
			visitor.visitColumnNumber(column, option.getColumnIndex() + 1);
			return;
		default:
			break;
		}

		assert valueType.useCell();
		Cell cell = currentRow.getCell(option.getColumnIndex());
		if (cell == null) {
			visitCellNull(column);
			return;
		}
		switch (valueType) {
		case CELL_VALUE:
		case CELL_FORMULA:
			visitCellValue(column, option, cell, visitor);
			return;
		case CELL_STYLE:
			visitCellStyle(column, option, cell, visitor);
			return;
		case CELL_FONT:
			visitCellFont(column, option, cell, visitor);
			return;
		case CELL_COMMENT:
			visitCellComment(column, option, cell, visitor);
			return;
		default:
			throw new UnsupportedOperationException(MessageFormat.format("unsupported value_type={0}", valueType));
		}
	}

	protected void visitCellNull(Column column) {
		pageBuilder.setNull(column);
	}

	protected void visitCellValue(Column column, ColumnOptionTask option, Cell cell, CellVisitor visitor) {
		PoiExcelCellVisitor delegator = new PoiExcelCellVisitor(visitorValue);
		delegator.visitCellValue(column, option, cell, visitor);
	}

	protected void visitCellStyle(Column column, ColumnOptionTask option, Cell cell, CellVisitor visitor) {
		PoiExcelCellStyleVisitor delegator = factory.getPoiExcelCellStyleVisitor();
		delegator.visitCellStyle(column, option, cell, visitor);
	}

	protected void visitCellFont(Column column, ColumnOptionTask option, Cell cell, CellVisitor visitor) {
		CellStyle style = cell.getCellStyle();
		Optional<List<String>> nameOption = option.getCellStyleName();
		if (!nameOption.isPresent()) {
			throw new RuntimeException(MessageFormat.format("cell_style_name must be specified. column.name={0}",
					column.getName()));
		}
		List<String> list = nameOption.get();
		String name = list.get(0); // TODO 全name

		PoiExcelCellFontVisitor delegator = factory.getPoiExcelCellFontVisitor();
		delegator.visitCellFont(column, style, name);
	}

	protected void visitCellComment(Column column, ColumnOptionTask option, Cell cell, CellVisitor visitor) {
		PoiExcelCellCommentVisitor delegator = factory.getPoiExcelCellCommentVisitor();
		delegator.visitCellComment(column, option, cell, visitor);
	}
}
