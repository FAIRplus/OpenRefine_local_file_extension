package com.refinepro;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.refine.RefineServlet;
import com.google.refine.commands.Command;
import com.google.refine.commands.HttpUtilities;
import com.google.refine.importing.*;
import com.google.refine.util.JSONUtilities;
import com.google.refine.util.ParsingUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.refine.importing.ImportingManager.Format;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class LocalFileSystemImportingController implements ImportingController {

    private static final Logger logger = LoggerFactory.getLogger("LocalFileSystemImportingController");

    protected RefineServlet servlet;

    private static final String LOCAL_FOLDER = System.getenv("EXT_LOCAL_FILE_SYSTEM") == null || System.getenv("EXT_LOCAL_FILE_SYSTEM").isEmpty() ? Paths.get(System.getProperty("user.home"), "Downloads").toString() : System.getenv("EXT_LOCAL_FILE_SYSTEM");

    @Override
    public void init(RefineServlet refineServlet) {
        this.servlet = refineServlet;
    }

    @Override
    public void doPost(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException, IOException {

        httpServletResponse.setCharacterEncoding("UTF-8");

        Properties parameters = ParsingUtilities.parseUrlParameters(httpServletRequest);

        String subCommand = parameters.getProperty("subCommand");
        logger.info("Subcommand : " + subCommand);
        if ("list-documents".equals(subCommand)) {
            doListDocuments(httpServletRequest, httpServletResponse, parameters);
        } else if ("load-raw-data".equals(subCommand)) {
            doLoadRawData(httpServletRequest, httpServletResponse, parameters);
        } else if ("update-file-selection".equals(subCommand)) {
            doUpdateFileSelection(httpServletRequest, httpServletResponse, parameters);
        } else if ("initialize-parser-ui".equals(subCommand)) {
            doInitializeParserUI(httpServletRequest, httpServletResponse, parameters);
        } else if ("update-format-and-options".equals(subCommand)) {
            doUpdateFormatAndOptions(httpServletRequest, httpServletResponse, parameters);
        } else if ("create-project".equals(subCommand)) {
            doCreateProject(httpServletRequest, httpServletResponse, parameters);
        } else {
            HttpUtilities.respond(httpServletResponse, "error", "No such sub command");
        }
    }

    @Override
    public void doGet(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException, IOException {
        HttpUtilities.respond(httpServletResponse, "error", "GET not implemented");

    }

    private void doListDocuments(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {

        Writer w = response.getWriter();
        JsonGenerator writer = ParsingUtilities.mapper.getFactory().createGenerator(w);
        try {
            writer.writeStartObject();
            writer.writeStringField("localPath", LOCAL_FOLDER);
            writer.writeArrayFieldStart("documents");

            try {
                listFiles(LOCAL_FOLDER, writer);
            } catch (Exception e) {
                logger.error("doListDocuments exception:" + e.getMessage());
            } finally {
                writer.writeEndArray();

            }
        } catch (IOException e) {
            throw new ServletException(e);
        } finally {
            writer.flush();
            writer.close();
            w.flush();
            w.close();
        }
    }

    private void listFiles(String folderPath, JsonGenerator writer)
            throws IOException {

        try {
            File f = new File(folderPath);

            if (f.isDirectory()) {
                File[] files = f.listFiles();

                for (int i = 0; i < Objects.requireNonNull(files).length; i++) {
                    File file = files[i];
                    if (file.canRead() && file.isFile() && !file.getName().contains(".~lock.")) {
                        writer.writeStartObject();
                        writer.writeStringField("name", file.getName());
                        writer.writeStringField("localPath", file.getAbsolutePath());
                        writer.writeEndObject();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void doLoadRawData(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {

        long jobID = Long.parseLong(parameters.getProperty("jobID"));
        ImportingJob job = ImportingManager.getJob(jobID);
        if (job == null) {
            HttpUtilities.respond(response, "error", "No such import job");
            return;
        }

        job.updating = true;
        ObjectNode config = job.getOrCreateDefaultConfig();
        if (!("new".equals(JSONUtilities.getString(config, "state", null)))) {
            HttpUtilities.respond(response, "error", "Job already started; cannot load more data");
            return;
        }

        LocalImportingUtilities.loadDataAndPrepareJob(
                request, response, parameters, job, config);
        job.touch();
        job.updating = false;
    }

    private void doUpdateFileSelection(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {

        long jobID = Long.parseLong(parameters.getProperty("jobID"));
        ImportingJob job = ImportingManager.getJob(jobID);
        if (job == null) {
            HttpUtilities.respond(response, "error", "No such import job");
            return;
        }

        job.updating = true;
        ObjectNode config = job.getOrCreateDefaultConfig();
        if (!("ready".equals(JSONUtilities.getString(config, "state", null)))) {
            HttpUtilities.respond(response, "error", "Job not ready");
            return;
        }

        ArrayNode fileSelectionArray = ParsingUtilities.evaluateJsonStringToArrayNode(
                request.getParameter("fileSelection"));

        ImportingUtilities.updateJobWithNewFileSelection(job, fileSelectionArray);

        replyWithJobData(request, response, job);
        job.touch();
        job.updating = false;
    }

    /**
     * return the job to the front end.
     *
     * @param request
     * @param response
     * @param job
     * @throws ServletException
     * @throws IOException
     */
    private void replyWithJobData(HttpServletRequest request, HttpServletResponse response, ImportingJob job)
            throws ServletException, IOException {

        Writer w = response.getWriter();
        ParsingUtilities.defaultWriter.writeValue(w, new JobResponse("ok", job));
        w.flush();
        w.close();
    }

    private void doUpdateFormatAndOptions(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {

        long jobID = Long.parseLong(parameters.getProperty("jobID"));
        ImportingJob job = ImportingManager.getJob(jobID);
        if (job == null) {
            HttpUtilities.respond(response, "error", "No such import job");
            return;
        }

        job.updating = true;
        ObjectNode config = job.getOrCreateDefaultConfig();
        if (!("ready".equals(JSONUtilities.getString(config, "state", null)))) {
            HttpUtilities.respond(response, "error", "Job not ready");
            return;
        }

        String format = request.getParameter("format");
        ObjectNode optionObj = ParsingUtilities.evaluateJsonStringToObjectNode(
                request.getParameter("options"));

        List<Exception> exceptions = new LinkedList<Exception>();

        ImportingUtilities.previewParse(job, format, optionObj, exceptions);

        Writer w = response.getWriter();
        JsonGenerator writer = ParsingUtilities.mapper.getFactory().createGenerator(w);
        try {
            writer.writeStartObject();
            if (exceptions.size() == 0) {
                job.project.update(); // update all internal models, indexes, caches, etc.

                writer.writeStringField("status", "ok");
            } else {
                writer.writeStringField("status", "error");
                writer.writeArrayFieldStart("errors");
                writeErrors(writer, exceptions);
                writer.writeEndArray();
            }
            writer.writeEndObject();
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new ServletException(e);
        } finally {
            w.flush();
            w.close();
        }
        job.touch();
        job.updating = false;
    }

    static public void writeErrors(JsonGenerator writer, List<Exception> exceptions) throws IOException {
        for (Exception e : exceptions) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            writer.writeStartObject();
            writer.writeStringField("message", e.getLocalizedMessage());
            writer.writeStringField("stack", sw.toString());
            writer.writeEndObject();
        }
    }

    private void doInitializeParserUI(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {

        long jobID = Long.parseLong(parameters.getProperty("jobID"));
        ImportingJob job = ImportingManager.getJob(jobID);
        if (job == null) {
            HttpUtilities.respond(response, "error", "No such import job");
            return;
        }

        String format = request.getParameter("format");
        Format formatRecord = ImportingManager.formatToRecord.get(format);
        if (formatRecord != null && formatRecord.parser != null) {
            ObjectNode options = formatRecord.parser.createParserUIInitializationData(
                    job, job.getSelectedFileRecords(), format);
            ObjectNode result = ParsingUtilities.mapper.createObjectNode();
            JSONUtilities.safePut(result, "status", "ok");
            JSONUtilities.safePut(result, "options", options);

            Command.respondJSON(response, result);
        } else {
            HttpUtilities.respond(response, "error", "Unrecognized format or format has no parser");
        }
    }

    private void doCreateProject(HttpServletRequest request, HttpServletResponse response, Properties parameters)
            throws ServletException, IOException {

        long jobID = Long.parseLong(parameters.getProperty("jobID"));
        ImportingJob job = ImportingManager.getJob(jobID);
        if (job == null) {
            HttpUtilities.respond(response, "error", "No such import job");
            return;
        }

        job.updating = true;
        job.touch();
        ObjectNode config = job.getOrCreateDefaultConfig();
        if (!("ready".equals(JSONUtilities.getString(config, "state", null)))) {
            HttpUtilities.respond(response, "error", "Job not ready");
            return;
        }

        String format = request.getParameter("format");
        ObjectNode optionObj = ParsingUtilities.evaluateJsonStringToObjectNode(
                request.getParameter("options"));

        List<Exception> exceptions = new LinkedList<Exception>();

        ImportingUtilities.createProject(job, format, optionObj, exceptions, false);

        HttpUtilities.respond(response, "ok", "done");
    }

    protected static class JobResponse {
        @JsonProperty("code")
        protected String code;
        @JsonProperty("job")
        protected ImportingJob job;

        protected JobResponse(String code, ImportingJob job) {
            this.code = code;
            this.job = job;
        }

    }
}
