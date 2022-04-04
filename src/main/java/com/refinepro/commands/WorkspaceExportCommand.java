package com.refinepro.commands;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.refine.ProjectManager;
import com.google.refine.browsing.Engine;
import com.google.refine.commands.Command;
import com.google.refine.exporters.*;
import com.google.refine.model.Project;
import com.google.refine.util.ParsingUtilities;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

public class WorkspaceExportCommand extends Command {

    private static final String LOCAL_FOLDER = System.getenv("EXT_LOCAL_FILE_SYSTEM") == null || System.getenv("EXT_LOCAL_FILE_SYSTEM").isEmpty() ? Paths.get(System.getProperty("user.home"), "Downloads").toString() : System.getenv("EXT_LOCAL_FILE_SYSTEM");

    static public Properties getRequestParameters(HttpServletRequest request) {
        Properties options = new Properties();

        Enumeration<String> en = request.getParameterNames();
        while (en.hasMoreElements()) {
            String name = en.nextElement();
            options.put(name, request.getParameter(name));
        }
        return options;
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        ProjectManager.singleton.setBusy(true);

        try {
            Project project = getProject(request);
            Engine engine = getEngine(request, project);
            Properties params = getRequestParameters(request);

            String format = params.getProperty("format");
            String name = params.getProperty("name", "no_name");
            name = name.trim().concat("(Export).").concat(format);
            String filePath = Paths.get(LOCAL_FOLDER, name).toString();
            Exporter exporter = ExporterRegistry.getExporter(format);
            if (exporter == null) {
                exporter = new CsvExporter('\t');
            }

            if (exporter instanceof WriterExporter) {
                String encoding = params.getProperty("encoding", "UTF-8");
                Writer writer = new OutputStreamWriter(new FileOutputStream(filePath), encoding);

                ((WriterExporter) exporter).export(project, params, engine, writer);
                writer.close();
            } else {
                respondException(response, new ServletException("Unknown exporter type"));
            }
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");

            Writer w = response.getWriter();
            JsonGenerator writer = ParsingUtilities.mapper.getFactory().createGenerator(w);

            writer.writeStartObject();
            writer.writeStringField("code", "ok");
            writer.writeStringField("message", "File '".concat(name).concat("' exported to the Workspace"));
            writer.writeEndObject();
            writer.flush();
            w.flush();
            w.close();
        } catch (Exception e) {
            // Use generic error handling rather than our JSON handling
            logger.info("error:{}", e.getMessage());
            throw new ServletException(e);
        } finally {
            ProjectManager.singleton.setBusy(false);
        }
    }
}
