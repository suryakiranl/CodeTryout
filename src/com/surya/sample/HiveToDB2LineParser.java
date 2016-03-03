package com.surya.sample;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveToDB2LineParser {
  private static Map<Integer, Integer> columnsToUpdate = new HashMap<>();
  public static void main(String[] args) {
    prepareColumnsMap();
    String inputFilePath = "C:\\Users\\slaskar\\Downloads\\data-comparison\\q02\\hdfs_data";
    String outputFilePath = inputFilePath + "-out";
    String line;
    int flushCount = 20000;
    try(BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {
      System.out.println("Inside try block.");
      int lineCount = 0;
      while( (line = br.readLine()) != null) {
        // System.out.println(">>> " + line);
        line = prepareLineForDB2InLine(line);
        // System.out.println("<<< " + line);
        bw.write(line);
        bw.newLine();
        lineCount++;
        if(lineCount % flushCount == 0) {
          System.out.println("Flushing data to file ...");
          bw.flush();
        }
      }
      System.out.println("Lines written = " + lineCount);
      bw.flush();
      System.out.println("File contents flushed. Exiting try block.");
    } catch (IOException e) {
      System.out.println("Error when executing program: " + e.getMessage());
      e.printStackTrace();
    } finally {
      System.out.println("Inside finally block.");
    }
  }

  private static void prepareColumnsMap() {
    List<Integer> columns = Arrays.asList(33, 2, 34, 35, 4 ,7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 23, 26, 29, 30);
    for(int col : columns) {
      columnsToUpdate.put(col, 1);
    }
  }

  private static String prepareLineForDB2InLine(String line) {
    StringBuilder sb = new StringBuilder(line);
    int delimCounter = 0;
    boolean doubleQuoteStarted = false;

    // If the first column needs to be wrapped in double quotes, add them first.
    int i = 0;
    if(columnsToUpdate.get(0) != null) {
      sb.insert(0, Constants.DOUBLE_QUOTE);
      i++;
      doubleQuoteStarted = true;
    }

    for (; i < sb.length(); i++) {
      if (sb.charAt(i) == Constants.HIVE_COLUMN_DELIM) {
        if(doubleQuoteStarted) { // Close the previously started double quote
          sb.insert(i++, Constants.DOUBLE_QUOTE);
          doubleQuoteStarted = false;
        }
        sb.setCharAt(i, Constants.DB2_COLUMN_DELIM); // Replace HIVE_DELIM with DB2_DELIM
        delimCounter++; // Bump up the delimited counter
        if(columnsToUpdate.get(delimCounter) != null) { // If the next column needs double quotes, start one now
          sb.insert(++i, Constants.DOUBLE_QUOTE);
          doubleQuoteStarted = true;
        }
      }
    }

    if(delimCounter < columnsToUpdate.size()) {
      System.out.println("delimiter count = " + delimCounter);
      System.out.println("Columns to update = " + columnsToUpdate);
    }

    return sb.toString();
  }
}
