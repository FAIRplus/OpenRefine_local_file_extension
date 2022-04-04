package com.refinepro;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.refine.importing.*;
import com.google.refine.util.JSONUtilities;
import com.google.refine.util.ParsingUtilities;
import org.apache.commons.fileupload.ProgressListener;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;

import javax.activation.MimetypesFileTypeMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.text.NumberFormat;
import java.util.*;

public class LocalImportingUtilities extends ImportingUtilities {


    public static void loadDataAndPrepareJob(HttpServletRequest request, HttpServletResponse response, Properties parameters, final ImportingJob job, ObjectNode config) throws IOException, ServletException {
        ObjectNode retrievalRecord = ParsingUtilities.mapper.createObjectNode();
        JSONUtilities.safePut(config, "retrievalRecord", retrievalRecord);
        JSONUtilities.safePut(config, "state", "loading-raw-data");
        final ObjectNode progress = ParsingUtilities.mapper.createObjectNode();
        JSONUtilities.safePut(config, "progress", progress);

        try {
            retrieveContentFromPostRequest(request, parameters, job.getRawDataDir(), retrievalRecord, new ImportingUtilities.Progress() {
                public void setProgress(String message, int percent) {
                    if (message != null) {
                        JSONUtilities.safePut(progress, "message", message);
                    }

                    JSONUtilities.safePut(progress, "percent", (long) percent);
                }

                public boolean isCanceled() {
                    return job.canceled;
                }
            });
        } catch (Exception var10) {
            JSONUtilities.safePut(config, "state", "error");
            JSONUtilities.safePut(config, "error", "Error uploading data");
            JSONUtilities.safePut(config, "errorDetails", var10.getLocalizedMessage());
            return;
        }

        ArrayNode fileSelectionIndexes = ParsingUtilities.mapper.createArrayNode();
        JSONUtilities.safePut(config, "fileSelection", fileSelectionIndexes);
        String bestFormat = autoSelectFiles(job, retrievalRecord, fileSelectionIndexes);
        bestFormat = guessBetterFormat(job, bestFormat);
        ArrayNode rankedFormats = ParsingUtilities.mapper.createArrayNode();
        rankFormats(job, bestFormat, rankedFormats);
        JSONUtilities.safePut(config, "rankedFormats", rankedFormats);
        JSONUtilities.safePut(config, "state", "ready");
        JSONUtilities.safePut(config, "hasData", true);
        config.remove("progress");
    }


    public static void retrieveContentFromPostRequest(HttpServletRequest request, Properties parameters, File rawDataDir, ObjectNode retrievalRecord, final Progress progress) throws Exception {
        ArrayNode fileRecords = ParsingUtilities.mapper.createArrayNode();
        JSONUtilities.safePut(retrievalRecord, "files", fileRecords);
        int clipboardCount = 0;
        int uploadCount = 0;
        int downloadCount = 0;
        int archiveCount = 0;
        final SavingUpdate update = new SavingUpdate() {
            public void savedMore() {
                progress.setProgress((String) null, calculateProgressPercent(this.totalExpectedSize, this.totalRetrievedSize));
            }

            public boolean isCanceled() {
                return progress.isCanceled();
            }
        };
        DiskFileItemFactory fileItemFactory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(fileItemFactory);
        upload.setProgressListener(new ProgressListener() {
            boolean setContentLength = false;
            long lastBytesRead = 0L;

            public void update(long bytesRead, long contentLength, int itemCount) {
                SavingUpdate var10000;
                if (!this.setContentLength && contentLength >= 0L) {
                    var10000 = update;
                    var10000.totalExpectedSize += contentLength;
                    this.setContentLength = true;
                }

                if (this.setContentLength) {
                    var10000 = update;
                    var10000.totalRetrievedSize += bytesRead - this.lastBytesRead;
                    this.lastBytesRead = bytesRead;
                    update.savedMore();
                }

            }
        });
        String localFile = request.getParameter("localFile");
        progress.setProgress("Uploading data ...", -1);

        File selectedFile = new File(localFile);
        InputStream stream = new FileInputStream(localFile);
        String fileName = selectedFile.getName();
        if (fileName.length() > 0) {

            File file = allocateFile(rawDataDir, fileName);
            FileUtils.copyFile(selectedFile, file);

            long fileSize = file.getTotalSpace();
            ObjectNode fileRecord = ParsingUtilities.mapper.createObjectNode();
            JSONUtilities.safePut(fileRecord, "origin", "upload");
            JSONUtilities.safePut(fileRecord, "declaredEncoding", request.getCharacterEncoding());
            MimetypesFileTypeMap fileTypeMap = new MimetypesFileTypeMap();
            JSONUtilities.safePut(fileRecord, "declaredMimeType", fileTypeMap.getContentType(file.getName()));
            JSONUtilities.safePut(fileRecord, "fileName", fileName);
            JSONUtilities.safePut(fileRecord, "location", getRelativePath(file, rawDataDir));
            progress.setProgress("Saving file " + fileName + " locally (" + formatBytes(fileSize) + " bytes)", calculateProgressPercent(update.totalExpectedSize, update.totalRetrievedSize));
            JSONUtilities.safePut(fileRecord, "size", fileSize);
            if (postProcessRetrievedFile(rawDataDir, file, fileRecord, fileRecords, progress)) {
                ++archiveCount;
            }

            ++uploadCount;
        }

        stream.close();


        JSONUtilities.safePut(retrievalRecord, "uploadCount", (long) uploadCount);
        JSONUtilities.safePut(retrievalRecord, "downloadCount", (long) downloadCount);
        JSONUtilities.safePut(retrievalRecord, "clipboardCount", (long) clipboardCount);
        JSONUtilities.safePut(retrievalRecord, "archiveCount", (long) archiveCount);
    }

    static public boolean postProcessRetrievedFile(
            File rawDataDir, File file, ObjectNode fileRecord, ArrayNode fileRecords, final Progress progress) {

        String mimeType = JSONUtilities.getString(fileRecord, "declaredMimeType", null);
        String contentEncoding = JSONUtilities.getString(fileRecord, "declaredEncoding", null);

        InputStream archiveIS = tryOpenAsArchive(file, mimeType, contentEncoding);
        if (archiveIS != null) {
            try {
                if (explodeArchive(rawDataDir, archiveIS, fileRecord, fileRecords, progress)) {
                    file.delete();
                    return true;
                }
            } finally {
                try {
                    archiveIS.close();
                } catch (IOException e) {
                    // TODO: what to do?
                }
            }
        }

        InputStream uncompressedIS = tryOpenAsCompressedFile(file, mimeType, contentEncoding);
        if (uncompressedIS != null) {
            try {
                File file2 = uncompressFile(rawDataDir, uncompressedIS, fileRecord, progress);

                file.delete();
                file = file2;
            } catch (IOException e) {
                // TODO: what to do?
                e.printStackTrace();
            } finally {
                try {
                    uncompressedIS.close();
                } catch (IOException e) {
                    // TODO: what to do?
                }
            }
        }

        postProcessSingleRetrievedFile(file, fileRecord);
        JSONUtilities.append(fileRecords, fileRecord);

        return false;
    }


    static String guessBetterFormat(ImportingJob job, String bestFormat) {
        ObjectNode retrievalRecord = job.getRetrievalRecord();
        return retrievalRecord != null ? guessBetterFormat(job, retrievalRecord, bestFormat) : bestFormat;
    }

    static String guessBetterFormat(ImportingJob job, ObjectNode retrievalRecord, String bestFormat) {
        ArrayNode fileRecords = JSONUtilities.getArray(retrievalRecord, "files");
        return fileRecords != null ? guessBetterFormat(job, fileRecords, bestFormat) : bestFormat;
    }

    static String guessBetterFormat(ImportingJob job, ArrayNode fileRecords, String bestFormat) {
        if (bestFormat != null && fileRecords != null && fileRecords.size() > 0) {
            ObjectNode firstFileRecord = JSONUtilities.getObjectElement(fileRecords, 0);
            String encoding = getEncoding(firstFileRecord);
            String location = JSONUtilities.getString(firstFileRecord, "location", (String) null);
            if (location != null) {
                File file = new File(location);

                while (true) {
                    String betterFormat = null;
                    List<FormatGuesser> guessers = (List) ImportingManager.formatToGuessers.get(bestFormat);
                    if (guessers != null) {
                        Iterator var9 = guessers.iterator();

                        while (var9.hasNext()) {
                            FormatGuesser guesser = (FormatGuesser) var9.next();
                            betterFormat = guesser.guess(file, encoding, bestFormat);
                            if (betterFormat != null) {
                                break;
                            }
                        }
                    }

                    if (betterFormat == null || betterFormat.equals(bestFormat)) {
                        break;
                    }

                    bestFormat = betterFormat;
                }
            }
        }
        return bestFormat;
    }

    static void rankFormats(ImportingJob job, final String bestFormat, ArrayNode rankedFormats) {
        final Map<String, String[]> formatToSegments = new HashMap();
        boolean download = bestFormat == null ? true : ((ImportingManager.Format) ImportingManager.formatToRecord.get(bestFormat)).download;
        List<String> formats = new ArrayList(ImportingManager.formatToRecord.keySet().size());
        Iterator var6 = ImportingManager.formatToRecord.keySet().iterator();

        String format;
        while (var6.hasNext()) {
            format = (String) var6.next();
            ImportingManager.Format record = (ImportingManager.Format) ImportingManager.formatToRecord.get(format);
            if (record.uiClass != null && record.parser != null && record.download == download) {
                formats.add(format);
                formatToSegments.put(format, format.split("/"));
            }
        }

        if (bestFormat == null) {
            Collections.sort(formats);
        } else {
            Collections.sort(formats, new Comparator<String>() {
                public int compare(String format1, String format2) {
                    if (format1.equals(bestFormat)) {
                        return -1;
                    } else {
                        return format2.equals(bestFormat) ? 1 : this.compareBySegments(format1, format2);
                    }
                }

                int compareBySegments(String format1, String format2) {
                    int c = this.commonSegments(format2) - this.commonSegments(format1);
                    return c != 0 ? c : format1.compareTo(format2);
                }

                int commonSegments(String format) {
                    String[] bestSegments = (String[]) formatToSegments.get(bestFormat);
                    String[] segments = (String[]) formatToSegments.get(format);
                    if (bestSegments != null && segments != null) {
                        int i;
                        for (i = 0; i < bestSegments.length && i < segments.length && bestSegments[i].equals(segments[i]); ++i) {
                        }

                        return i;
                    } else {
                        return 0;
                    }
                }
            });
        }

        var6 = formats.iterator();

        while (var6.hasNext()) {
            format = (String) var6.next();
            rankedFormats.add(format);
        }

    }

    private abstract static class SavingUpdate {
        public long totalExpectedSize;
        public long totalRetrievedSize;

        private SavingUpdate() {
            this.totalExpectedSize = 0L;
            this.totalRetrievedSize = 0L;
        }

        public abstract void savedMore();

        public abstract boolean isCanceled();
    }

    private static int calculateProgressPercent(long totalExpectedSize, long totalRetrievedSize) {
        return totalExpectedSize == 0L ? -1 : (int) (totalRetrievedSize * 100L / totalExpectedSize);
    }

    private static String formatBytes(long bytes) {
        return NumberFormat.getIntegerInstance().format(bytes);
    }
}
