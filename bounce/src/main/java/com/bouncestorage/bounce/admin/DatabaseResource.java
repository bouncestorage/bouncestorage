/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Class to implement an endpoint for GET queries against InfluxDB.
 */
@Path("/db")
@Produces(MediaType.APPLICATION_JSON)
public class DatabaseResource {
    @Path("{database}")
    @GET
    public Response getSeries(@PathParam("database") String database,
                            @QueryParam("username") String username,
                            @QueryParam("password") String password,
                            @QueryParam("query") String query) {

        HttpURLConnection httpConn;
        try {
            String url = String.format("%s/db/%s/series?u=%s&p=%s&q=%s", BounceStats.ENDPOINT, database, username,
                    password, URLEncoder.encode(query, "UTF8"));
            httpConn = (HttpURLConnection) new URL(url).openConnection();
            if (httpConn.getResponseCode() != Response.Status.OK.getStatusCode()) {
                throw new WebApplicationException(httpConn.getResponseMessage(), httpConn.getResponseCode());
            }
            Response response = Response.ok(httpConn.getInputStream()).build();
            return response;
        } catch (IOException e) {
            throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
        }
    }
}
