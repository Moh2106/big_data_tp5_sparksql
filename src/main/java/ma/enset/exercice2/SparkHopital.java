package ma.enset.exercice2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHopital {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("SPARQ SQL HOPITAL").master("local").getOrCreate();

        Dataset<Row> dfConsultation = ss.read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_hopital")
                .option("user", "root")
                .option("query", "select date_consultation, count(*) as nombre_de_consultation from consultations group by date_consultation")
                .option("password", "")
                .load();

        dfConsultation.show();

        Dataset<Row> dfConsultationParMedecin = ss.read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_hopital")
                .option("user", "root")
                .option("query", "SELECT nom, prenom, COUNT(*) as nombre_de_consultation FROM medecins, consultations WHERE medecins.id = consultations.id_medecin GROUP BY nom, prenom")
                .option("password", "")
                .load();

        dfConsultationParMedecin.show();

    }
}
