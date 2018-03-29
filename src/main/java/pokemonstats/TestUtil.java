package pokemonstats;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class TestUtil {
    public static List<Pair<Text,Text>> readXlsx(String path) {
        LinkedList<Pair<Text,Text>> pokemonRow = new LinkedList<Pair<Text,Text>>();
        try  {
            FileInputStream file = new FileInputStream(new File(path));
            XSSFWorkbook wb = new XSSFWorkbook(file);
            XSSFSheet sheet = wb.getSheetAt(0);
            System.out.println();
            Iterator<Row> rowIterator = sheet.rowIterator();
            while(rowIterator.hasNext()){
                XSSFRow row = (XSSFRow) rowIterator.next();
                Iterator<Cell> cellIterator = row.cellIterator();
                String cellValue = "";
                while(cellIterator.hasNext()){
                    XSSFCell cell = (XSSFCell) cellIterator.next();
                    if(cell.getCellType() == Cell.CELL_TYPE_STRING)
                        cellValue += String.valueOf(cell.getStringCellValue())+",";
                    else if(cell.getCellType() == Cell.CELL_TYPE_NUMERIC)
                        cellValue += String.valueOf(cell.getNumericCellValue())+",";
                    else
                        cellValue += String.valueOf(cell.getRawValue())+",";

                }
                pokemonRow.add(new Pair<Text,Text>(new Text(), new Text(cellValue)));
            }

        }catch(FileNotFoundException e) {
            System.out.println(e.getMessage());
            return null;
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            return null;
        }
        return pokemonRow;
    }

    public static void Output(List<Pair<Text, Text>> output) {
        File file = new File("src/main/resources/output.csv");
        try {
            FileOutputStream outputStream = new FileOutputStream(file);
            byte[] headers = ("type,tank,feeble,defender,slowpoke\n").getBytes();
            outputStream.write(headers, 0, headers.length);
            for (Pair<Text, Text> pair : output) {
                byte[] buffer = (pair.getFirst().toString() + "," + pair.getSecond().toString()+"\n").getBytes();
                outputStream.write(buffer, 0, buffer.length);
            }
        }catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

}
