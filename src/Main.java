import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import model.BuildingModel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class Main {

    private static Connection connection;

    public static void main(String[] args) throws IOException, CsvException {

        String fileName = "src/resources/base_sankt.csv";
        final String databaseName = "src/resources/buildings.db";
        Path path = Paths.get(fileName);

        CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + databaseName);

            try (var br = Files.newBufferedReader(path,  StandardCharsets.UTF_8);
                 var reader = new CSVReaderBuilder(br).withCSVParser(parser).build()) {

                List<String[]> rows = reader.readAll();
                rows.remove(0);

                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM buildings");

                if (resultSet.getInt(1) != rows.size()) {
                    writeToBase(rows);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        firstTask();
        secondTask();
        ThirdTask();
    }

    private static void addBuilding(BuildingModel building) {
        try {
            PreparedStatement insertStmt = connection.prepareStatement(
                    "INSERT INTO buildings(number, address, snapshot, appellation, number_of_floor, prefix_code, building_type, id_, year_construction) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
            insertStmt.setString(1, building.number);
            insertStmt.setString(2, building.address);
            insertStmt.setString(3, building.snapshot);
            insertStmt.setString(4, building.appellation);
            insertStmt.setString(5, building.numberOfFloor);
            insertStmt.setInt(6, building.prefixCode);
            insertStmt.setString(7, building.buildingType);
            insertStmt.setInt(8, building.id);
            insertStmt.setString(9, building.yearConstruction);
            insertStmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void firstTask() {
        Map<String, Integer> results ;

        try {
            Statement statement = connection.createStatement();
            results = getFirstTaskData(statement);

            var dataset = new DefaultCategoryDataset();
            for (var key : results.keySet()) {
                dataset.addValue(results.get(key), key, "");
            }

            var barChart = ChartFactory.createBarChart(
                    "Зависимость кол-ва домов от этажности",
                    "Этаж",
                    "Кол-во домов",
                    dataset,
                    PlotOrientation.VERTICAL,
                    true, false, false);
            try {
                ChartUtils.saveChartAsPNG(new File("screens/task_1_graphic.png"), barChart, 640, 520);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void secondTask() {
        List<BuildingModel> result;
        try {
            Statement statement = connection.createStatement();
            result = getSecondTaskData(statement);
            System.out.println("Ответ на 2 задачу:");
            for (var item : result) {
                System.out.print(item.number + ", ");
                System.out.print(item.address + ", ");
                System.out.print(item.snapshot + ", ");
                System.out.print(item.appellation + ", ");
                System.out.print(item.numberOfFloor + ", ");
                System.out.print(item.prefixCode + ", ");
                System.out.print(item.buildingType + ", ");
                System.out.print(item.id + ", ");
                System.out.print(item.yearConstruction);
                System.out.println();
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void ThirdTask() {
        int result = 0;
        try {
            Statement statement = connection.createStatement();
            result = getThirdTaskData(statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Ответ на 3 задачу: " + result);
    }

    private static void writeToBase(List<String[]> rows) {
        List<BuildingModel> buildingsList = new ArrayList<BuildingModel>();

        for (var i = 0; i < rows.size(); i++) {
            BuildingModel building = new BuildingModel();
            String[] row = rows.get(i);

            building.number = row[0];
            building.address = row[1];
            building.snapshot = row[2];
            building.appellation = row[3];
            building.numberOfFloor = row[4];
            building.prefixCode = Integer.parseInt(row[5]);
            building.buildingType = row[6];
            building.id = Integer.parseInt(row[7]);
            building.yearConstruction = row[8];
            buildingsList.add(building);

            addBuilding(buildingsList.get(i));
            System.out.println("id: " + i + ". Add building");
        }
    }

    private static Map<String, Integer> getFirstTaskData(Statement statement) {
        Map<String, Integer> results = new HashMap<String, Integer>();

        try {
            ResultSet rs_floors = statement.executeQuery("SELECT COUNT(*), number_of_floor FROM buildings GROUP BY number_of_floor ORDER BY number_of_floor");
            while (rs_floors.next()) {
                if (!rs_floors.getString(2).equals("")) {
                    results.put(rs_floors.getString(2), rs_floors.getInt(1));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Map<String, Integer> resultsSorted = new TreeMap<String, Integer>();
        resultsSorted.putAll(results);
        return resultsSorted;
    }

    private static List<BuildingModel> getSecondTaskData(Statement statement) {
        List<BuildingModel> buildings = new ArrayList<>();
        try {
            ResultSet rs_adress = statement.executeQuery("SELECT * FROM buildings WHERE address LIKE '%Шлиссельбургское шоссе%' AND appellation = 'Зарегистрированный участок' AND prefix_code = '9881'");
            while (rs_adress.next()) {
                BuildingModel building = new BuildingModel();
                building.number = rs_adress.getString(1);
                building.address = rs_adress.getString(2);
                building.snapshot = rs_adress.getString(3);
                building.appellation = rs_adress.getString(4);
                building.numberOfFloor = rs_adress.getString(5);
                building.prefixCode = rs_adress.getInt(6);
                building.buildingType = rs_adress.getString(7);
                building.id = rs_adress.getInt(8);
                building.yearConstruction = rs_adress.getString(9);
                buildings.add(building);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return buildings;
    }

    private static int getThirdTaskData(Statement statement) {
        int sumPrefixCode = 0;
        int countPrefixCode = 0;
        int avgPrefixCode = 0;

        try {
            ResultSet rs_avg_prefix = statement.executeQuery("SELECT prefix_code FROM buildings WHERE (appellation LIKE '%университет%' OR appellation LIKE '%Университет%') AND number_of_floor > '5-этажный' AND year_construction != ''");
            while (rs_avg_prefix.next()) {
                sumPrefixCode += rs_avg_prefix.getInt(1);
                countPrefixCode += 1;
            }
            avgPrefixCode = (Integer) sumPrefixCode / countPrefixCode;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return avgPrefixCode;
    }
}
