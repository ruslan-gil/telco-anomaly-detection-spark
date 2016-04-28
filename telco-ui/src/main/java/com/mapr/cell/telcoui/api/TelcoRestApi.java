package com.mapr.cell.telcoui.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.cell.common.DAO;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;


@Path("/telco/statistics")
public class TelcoRestApi {
    private DAO dao = DAO.getInstance();
    private Table table;


    @GET
    @Produces(APPLICATION_JSON)
    public Response getData() throws JsonProcessingException {

        List<Document> items = new ArrayList<>();
        ObjectMapper object = new ObjectMapper();
        table = dao.getStatsTable();

        QueryCondition c = MapRDB.newCondition().is("simulationId", QueryCondition.Op.EQUAL, dao.getLastSimulationID());
        DocumentStream rs = table.find(c);

        if (rs != null) {
            for(Document doc : rs) {
                items.add(doc);
                System.out.println(doc.asJsonString());
            }
            rs.close();
        }
        return Response.ok(object.writeValueAsString(items)).build();
    }
}
