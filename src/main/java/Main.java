import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/abc";
    private static final String USER = "postgres";
    private static final String PASS = "postgres";
    private static final String XMLFile = "organization.xml";

    public static void main(String[] args) {

        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASS)) {
            if (connection == null) {
                System.out.println("Connection is null");
                return;
            }
            List<String> orgFromXML = getTitlesByTagFromXML(new File(XMLFile), "organization");
            if (orgFromXML == null || orgFromXML.isEmpty()) {
                return;
            }
            List<String> orgFromDB = getEntityTitlesFromDB(connection, "organization");
            orgFromXML.removeAll(orgFromDB);
            if (!orgFromDB.isEmpty()) {
                connection.setAutoCommit(false);
                saveOrganization(connection, orgFromXML);
            }
        } catch (SQLException e) {
            System.out.println("Connection Failed");
            e.printStackTrace();
        }
    }

    public static List<String> getEntityTitlesFromDB(Connection conn, String tableName) {
        List<String> orgFromDB = new ArrayList<>();
        String query = String.format("SELECT * FROM %s", tableName);
        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.execute();
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                orgFromDB.add(resultSet.getString("title"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return orgFromDB;
    }

    public static List<String> getTitlesByTagFromXML(File file, String tagEntity) {
        List<String> organizations = new ArrayList<>();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(file);
            NodeList orgElements = document.getDocumentElement().getElementsByTagName(tagEntity);
            for (int i = 0; i < orgElements.getLength(); i++) {
                Node org = orgElements.item(i);
                NamedNodeMap attributes = org.getAttributes();
                organizations.add(attributes.getNamedItem("title").getNodeValue());
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
            return null;
        }
        return organizations;
    }

    public static void saveOrganization(Connection connection, List<String> organizations) {
        try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO organization (title) VALUES (?)")) {
            for (String org : organizations) {
                pstmt.setString(1, org);
                pstmt.addBatch();
            }
            try {
                pstmt.executeBatch();
                connection.commit();
            } catch (BatchUpdateException ex) {
                connection.rollback();
                ex.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}