package com.mapr.cell.telcoui;

import com.mapr.cell.telcoui.api.RealTimeApi;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;


public class Main {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws Exception {
	
	    String httpPort = System.getProperty("demo.http.port", "8080");
    
	
        LOG.info("================================================");
        LOG.info("   Starting Telco UI on port "+ httpPort);
        LOG.info("================================================\n\n");

        Server server = new Server(Integer.parseInt(httpPort));


        ServletHolder sh = new ServletHolder(ServletContainer.class);
        // Set the package where the services reside
        sh.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "com.mapr.cell.telcoui.api");
        
        sh.setInitOrder(1); // force loading at startup
        
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setResourceBase("./telco-ui/src/main/resources/webapp");

        ServletContextHandler sch = new ServletContextHandler();
                sch.addServlet(sh, "/*");
                sch.addServlet(new ServletHolder(new RealTimeApi()), "/talk");
        

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resourceHandler, sch});
        server.setHandler(handlers);

        server.start();
        server.join();
    }

}