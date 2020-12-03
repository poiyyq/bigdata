package com.winnie.util;

import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ExcelUtils {
    public static List<Map<String, Object>> getData(String filePath) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();
        Workbook wb = HSSFWorkbookFactory.create(new File(filePath));
        Sheet sheetAt = wb.getSheetAt(0);
        int physicalNumberOfRows = sheetAt.getPhysicalNumberOfRows();

        // 获取列宽
        int size = sheetAt.getRow(0).getPhysicalNumberOfCells();
        // 获取title
        List<String> keyTitle = getTitle(sheetAt);
        // 组装数据对象
        for (int i = 1; i < physicalNumberOfRows; i++) {
            Row row = sheetAt.getRow(i);
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < size; j++) {
                Cell cell = row.getCell(j);
                String key = keyTitle.get(j);
                map.put(key, cell.getStringCellValue());
            }
            list.add(map);
        }
        wb.close();
        return list;
    }

    /**
     * 获取title集合
     * @param sheetAt
     * @return
     */
    private static List<String> getTitle(Sheet sheetAt) {
        Row row = sheetAt.getRow(0);
        int physicalNumberOfCells = row.getPhysicalNumberOfCells();
        List<String> keyTitle = new ArrayList<>();
        for (int i = 0; i < physicalNumberOfCells; i++) {
            keyTitle.add(row.getCell(i).getStringCellValue());
        }
        return keyTitle;
    }
}
