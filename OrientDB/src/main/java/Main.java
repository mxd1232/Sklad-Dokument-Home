import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

        ODatabaseSession db = orient.open("miki", "miki", "miki");

        Scanner scanner = new Scanner(System.in);
        int select;
        createSchema(db);
        do {
            System.out.println("[0]wyjscie\t\t[1]dodaj kino\t\t[2]usun kino\t\t[3]znajdz po id\t\t[4]aktualizuj kino\t\t[5]znajdz po nazwie\t\t" +
                    "[6]srednia liczba pracownikow kina po lokalizacji\\t\"");
            select = scanner.nextInt();
            switch (select) {
                case 0:
                    break;
                case 1:
                createCinema(db);
                    break;
                case 2:
                    deleteCinemaById( db);
                    break;
                case 3:
                    getCinemaById( db);
                    break;
                case 4:
                    updateCinema(db);
                    break;
                case 5:
                    getCinemaByName(db);
                    break;
                case 6:
                    findAvgWorkersNumberByLocation(db);
                    break;
                default:
                    throw new IllegalStateException("Nie ma takiej opcji pod tym numerem: " + select);
            }
        } while (select != 0);

        db.close();
        orient.close();

    }

    private static void createSchema(ODatabaseSession db) {
        OClass person = db.getClass("Cinema");

        if (person == null) {
            person = db.createVertexClass("Cinema");
        }

        if (person.getProperty("name") == null) {
            person.createProperty("name", OType.STRING);
            person.createIndex("Cinema_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "name");
        }
    }


    private static OVertex createCinema(ODatabaseSession db) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Podaj id:");
        String id = scanner.nextLine();

        System.out.println("Podaj nazwe kina:");
        String name = scanner.nextLine();

        System.out.println("Podaj lokalizacje:");
        String location = scanner.nextLine();

        System.out.println("Podaj liczbe pracownikow:");
        int workersNumber = scanner.nextInt();

        OVertex result = db.newVertex("Cinema");
        result.setProperty("id",id);
        result.setProperty("name", name);
        result.setProperty("location", location);
        result.setProperty("workersNumber", workersNumber);
        result.save();
        return result;
    }

    private static void getCinemaByName(ODatabaseSession db) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Podaj nazwe kina:");
        String name = scanner.nextLine();


        String query = "SELECT * from Cinema where name = ?";
        OResultSet rs = db.query(query,name );

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("kino: " + item);
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

    private static void getCinemaById(ODatabaseSession db) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Podaj id:");
        String id = scanner.nextLine();

        try {
            String query = "Select * from Cinema  where id = ?";
            OResultSet rs = db.query(query, id);

            while (rs.hasNext()) {
                OResult item = rs.next();
                System.out.println("kino: " + item);
            }
            rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        } catch (Exception e) {
            System.out.println("Nie ma kina o takim id.");
          //  rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        }
    }

    private static void deleteCinemaById(ODatabaseSession db) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Podaj id:");
        String id = scanner.nextLine();


        String query = "DELETE VERTEX FROM Cinema where id = ?";
        OResultSet rs = db.command(query, id);
        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

    private static void updateCinema(ODatabaseSession db) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Podaj id:");
        String id = scanner.nextLine();


        String query = "DELETE VERTEX FROM Cinema where id = ?";
        OResultSet rs = db.command(query, id);
        System.out.println("Podaj id:");
        id = scanner.nextLine();

        System.out.println("Podaj nazwe kina:");
        String name = scanner.nextLine();

        System.out.println("Podaj lokalizacje:");
        String location = scanner.nextLine();

        System.out.println("Podaj liczbe pracownikow:");
        int workersNumber = scanner.nextInt();

        OVertex result = db.newVertex("Cinema");
        result.setProperty("id",id);
        result.setProperty("name", name);
        result.setProperty("location", location);
        result.setProperty("workersNumber", workersNumber);
        result.save();
        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

    private static void findAvgWorkersNumberByLocation(ODatabaseSession db) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Podaj lokalizacje:");
        String location = scanner.nextLine();


            String query = "Select * from Cinema  where location = ?";
            OResultSet rs = db.query(query, location);

            int count=0, sum = 0;
            while (rs.hasNext()) {
                OResult item = rs.next();
                int convertedValue = item.getProperty("workersNumber");
                sum = sum + convertedValue;
                count = count + 1;
            }
            System.out.println("Srednia liczba pracowników: " + sum/count + " dla lokalizacji: " + location);
            rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }
}