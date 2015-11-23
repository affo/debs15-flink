package it.affo.phd.debs15.flink;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by affo on 23/11/15.
 */
public class TaxiRide {
    private static DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 2013-01-01 00:02:00
    private static final int NO_FIELDS = 17;

    public Date pickupTS, dropoffTS;
    public String pickupCell, dropoffCell;
    public String taxiID;
    public double fare, tip;

    /**
     * @param line the line to be parsed in csv format. E.g.
     *             DFBFA82ECA8F7059B89C3E8B93DAA377,
     *             CF8604E72D83840FBA1978C2D2FC9CDB,
     *             2013-01-01 00:02:00,
     *             2013-01-01 00:03:00,
     *             60,
     *             0.39,
     *             -73.981544,
     *             40.781475,
     *             -73.979439,
     *             40.784386,
     *             CRD,
     *             3.00,0.50,0.50,0.70,0.00,4.70
     * @return null if there was any error while parsing; a TaxiRide object instead.
     */
    public static TaxiRide parseLine(String line) {
        String[] tokens = line.split(",");

        if (tokens.length != NO_FIELDS) {
            return null;
        }

        String taxiID = tokens[0];
        try {

            Date pickupTS = dateFmt.parse(tokens[2]);
            Date dropoffTS = dateFmt.parse(tokens[3]);

            double puLat = Double.valueOf(tokens[7]);
            double puLong = Double.valueOf(tokens[6]);
            double doLat = Double.valueOf(tokens[9]);
            double doLong = Double.valueOf(tokens[8]);

            String pickupCell = AreaMapper.getCellID(puLat, puLong);
            String dropoffCell = AreaMapper.getCellID(doLat, doLong);


            double fare = Double.valueOf(tokens[11]);
            double tip = Double.valueOf(tokens[14]);

            TaxiRide tr = new TaxiRide();
            tr.pickupTS = pickupTS;
            tr.dropoffTS = dropoffTS;
            tr.pickupCell = pickupCell;
            tr.dropoffCell = dropoffCell;
            tr.taxiID = taxiID;
            tr.fare = fare;
            tr.tip = tip;
            return tr;

        } catch (AreaMapper.OutOfGridException |
                ParseException |
                NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "TaxiRide {" +
                "\n\tpickupTS = " + dateFmt.format(pickupTS) +
                ",\n\tdropoffTS = " + dateFmt.format(dropoffTS) +
                ",\n\tpickupCell = '" + pickupCell + '\'' +
                ",\n\tdropoffCell = '" + dropoffCell + '\'' +
                ",\n\ttaxiID = '" + taxiID + '\'' +
                ",\n\tfare = " + fare +
                ",\n\ttip = " + tip +
                "\n}\n";
    }
}
