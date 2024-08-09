package gcfv2;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import com.monitorjbl.xlsx.StreamingReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.io.*;
import java.util.*;
import java.util.zip.ZipInputStream;
import java.io.BufferedWriter;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ExcelProcessor implements HttpFunction {
  private static final Gson gson = new Gson();
  private static final Logger logger = Logger.getLogger(ExcelProcessor.class.getName());
  private static final AtomicInteger sheetCounter = new AtomicInteger(0);
  
  @Override
  public void service(final HttpRequest request, final HttpResponse response) throws Exception {
    final BufferedWriter writer = response.getWriter();

    // Parse the JSON payload
    JsonObject payload = gson.fromJson(request.getReader(), JsonObject.class);
    
    String driveFileId = payload.get("zipped-file-id").getAsString();
    String projectId = payload.get("project-id").getAsString();
    String datasetId = payload.get("bq-dataset-id").getAsString();
    String tableName = payload.get("bq-main-table").getAsString();
    String outputFileName = payload.get("output-file-name").getAsString();
    String bucketName = payload.get("bucket-name").getAsString();
    String avroSchemaJson = payload.get("avro-schema").getAsString();
    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    String keyMappingJson = payload.get("key-mapping").getAsString();
    Map<String, String> keyMapping = gson.fromJson(keyMappingJson, new TypeToken<Map<String, String>>(){}.getType());

    
    logger.info(String.format("Downloading zip file"));
    byte[] unzippedContent = downloadAndUnzipFile(driveFileId);

    logger.info(String.format("Starting to process excel file"));
    processExcelFile(unzippedContent, outputFileName, bucketName, projectId, datasetId, tableName, avroSchema, keyMapping);

    JsonObject jsonResponse = new JsonObject();
    jsonResponse.addProperty("status", "success");
    jsonResponse.addProperty("message", "File processed successfully");

    response.setContentType("application/json");

    writer.write(gson.toJson(jsonResponse));
    writer.flush();
  }
  
  private void processExcelFile(byte[] excelContent, String outputFileName, String bucketName, String projectId, String datasetId, String tableName, Schema avroSchema, Map<String, String> keyMapping) throws IOException {

    double sizeInMB = excelContent.length / (1024.0 * 1024.0);
    logger.info(String.format("Excel content size: %.2f MB", sizeInMB));

    ExecutorService executorService = Executors.newFixedThreadPool(8); // Assuming 8 vCPUs
    ConcurrentLinkedQueue<Future<String>> futures = new ConcurrentLinkedQueue<>();
    List<String> avroFileNames = new ArrayList<>();

    try (InputStream is = new ByteArrayInputStream(excelContent);
          Workbook workbook = StreamingReader.builder()
            .rowCacheSize(100)
            .bufferSize(4096)
            .open(is)) {
  
      for (Sheet sheet : workbook) {
        if (sheet.getSheetName().startsWith("Sponsored Products Campaigns")) {
            futures.add(executorService.submit(createSheetProcessor(sheet, outputFileName, bucketName, avroSchema, keyMapping)));
        }
      }

      while (!futures.isEmpty()) {
        Future<String> future = futures.poll();
        try {
            String result = future.get();
            avroFileNames.add(result);
            logger.info("Processed and uploaded: " + result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Processing was interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Error processing sheet", e.getCause());
        }
      }

      createBigQueryLoadJob(projectId, datasetId, tableName, bucketName, avroFileNames);

    } catch (Exception e) {
      logger.log(Level.SEVERE, "Error processing Excel file", e);
      throw new IOException("Error processing Excel file", e);
    } finally {
      executorService.shutdown();
    }
  }

  private Callable<String> createSheetProcessor(Sheet sheet, String outputFileName, String bucketName, Schema avroSchema, Map<String, String> keyMapping) {
    return () -> {
      String sheetName = sheet.getSheetName();
      String avroFileName = generateAvroFileName(outputFileName, sheetName);
      ByteArrayOutputStream avroOutputStream = new ByteArrayOutputStream();
    
      try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(avroSchema))) {
        dataFileWriter.create(avroSchema, avroOutputStream);
    
        processSheet(sheet, dataFileWriter, avroSchema, keyMapping);
    
        dataFileWriter.flush();
      }

      uploadToGCS(avroOutputStream.toByteArray(), bucketName, avroFileName);

      return avroFileName;
    };
  }

  
  private void processSheet(Sheet sheet, DataFileWriter<GenericRecord> dataFileWriter, Schema avroSchema, Map<String, String> keyMapping) throws IOException {
    String sheetName = sheet.getSheetName();
    logger.info("Processing sheet: " + sheetName);
    
    Iterator<Row> rowIterator = sheet.iterator();
    if (!rowIterator.hasNext()) {
      logger.warning("Sheet is empty: " + sheetName);
      return;
    }

    Row headerRow = rowIterator.next();
    Map<Integer, String> columnMapping = createColumnMapping(headerRow, keyMapping);
    
    int rowsProcessed = 0;
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      GenericRecord record = processRow(row, columnMapping, avroSchema);
      dataFileWriter.append(record);
      rowsProcessed++;
      
      if (rowsProcessed % 200000 == 0) {
        logger.info("Processed " + rowsProcessed + " rows in sheet: " + sheetName);

      }
    }
    
    logger.info("Finished processing sheet: " + sheetName + ". Total rows processed: " + rowsProcessed);
  }

  private Map<Integer, String> createColumnMapping(Row headerRow, Map<String, String> keyMapping) {
    Map<Integer, String> columnMapping = new HashMap<>();
    for (Cell cell : headerRow) {
      String headerValue = cell.getStringCellValue();
      columnMapping.put(cell.getColumnIndex(), keyMapping.getOrDefault(headerValue, headerValue));
    }
    return columnMapping;
  }

  private GenericRecord processRow(Row row, Map<Integer, String> columnMapping, Schema avroSchema) {
    GenericRecord record = new GenericData.Record(avroSchema);

    for (Cell cell : row) {
      int colNum = cell.getColumnIndex();
      String columnName = columnMapping.get(colNum);

      if (columnName != null) {
        Schema.Field field = avroSchema.getField(columnName);
        Schema fieldSchema = field.schema();
        Schema.Type fieldType = fieldSchema.getType();

        if (fieldType == Schema.Type.UNION) {
          fieldSchema = fieldSchema.getTypes().stream()
            .filter(s -> s.getType() != Schema.Type.NULL)
            .findFirst()
            .orElse(fieldSchema.getTypes().get(0));
          fieldType = fieldSchema.getType();
        }

        boolean isDateField = columnName.equals("start_date") || columnName.equals("end_date");

        if (isDateField) {
          if (cell.getCellType() == CellType.STRING) {
            String stringValue = cell.getStringCellValue().trim();
            try {
              DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
              LocalDate date = LocalDate.parse(stringValue, formatter);
              long daysSinceEpoch = ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
              record.put(columnName, (int) daysSinceEpoch);
            } catch (DateTimeParseException e) {
              record.put(columnName, null);
            }
          } else if (cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
            LocalDate date = cell.getLocalDateTimeCellValue().toLocalDate();
            long daysSinceEpoch = ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
            record.put(columnName, (int) daysSinceEpoch);
          } else {
            double numericValue = cell.getNumericCellValue();
            record.put(columnName, (int) numericValue);
          }
        } else {
          switch (cell.getCellType()) {
            case BLANK:
              record.put(columnName, null);
              break;
            case STRING:
              String stringValue = cell.getStringCellValue().trim();
              record.put(columnName, stringValue.isEmpty() ? null : stringValue);
              break;
            case NUMERIC:
              double numericValue = cell.getNumericCellValue();
              if (fieldType == Schema.Type.LONG) {
                record.put(columnName, (long) numericValue);
              } else if (fieldType == Schema.Type.DOUBLE) {
                record.put(columnName, numericValue);
              } else if (fieldType == Schema.Type.INT) {
                record.put(columnName, (int) numericValue);
              } else {
                record.put(columnName, String.valueOf(numericValue));
              }             
              break;
            case BOOLEAN:
              record.put(columnName, cell.getBooleanCellValue());
              break;
            case FORMULA:
              record.put(columnName, cell.getCellFormula());
              break;
            default:
              record.put(columnName, null);
          }
        }
      }
    }
    return record;
  }

  private void createBigQueryLoadJob(String projectId, String datasetId, String tableName, String bucketName, List<String> avroFileNames) {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    TableId tableId = TableId.of(datasetId, tableName);
    List<String> sourceUris = new ArrayList<>();
    for (String avroFileName : avroFileNames) {
      sourceUris.add(String.format("gs://%s/%s", bucketName, avroFileName));
    }

    LoadJobConfiguration configuration = LoadJobConfiguration
      .newBuilder(tableId, sourceUris)
      .setUseAvroLogicalTypes(true) 
      .setFormatOptions(FormatOptions.avro())
      .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
      .build();

    JobId jobId = JobId.of(projectId, tableId.getDataset() + "-" + tableId.getTable() + "-" + System.currentTimeMillis());

    Job loadJob = bigquery.create(JobInfo.newBuilder(configuration).setJobId(jobId).build());

    try {
      Job completedJob = loadJob.waitFor();
      if (completedJob == null) {
        throw new RuntimeException("Job no longer exists");
      } else if (completedJob.getStatus().getError() != null) {
        throw new RuntimeException(completedJob.getStatus().getError().toString());
      } else {
        logger.info("BigQuery load job completed successfully");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Job was interrupted", e);
    }
  }

  private byte[] downloadAndUnzipFile(String driveFileId) throws Exception {
    // Set up Google Drive service
    NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault()
      .createScoped("https://www.googleapis.com/auth/drive.readonly");
    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);

    Drive driveService = new Drive.Builder(httpTransport, JacksonFactory.getDefaultInstance(), requestInitializer)
      .setApplicationName("Excel Processor")
      .build();

    logger.info("downloading file");
    // Download file from Google Drive
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    driveService.files().get(driveFileId)
      .executeMediaAndDownloadTo(outputStream);
    logger.info(String.format("downloaded"));

    // Unzip the file
    try (ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
      ByteArrayOutputStream unzippedOutputStream = new ByteArrayOutputStream()) {

      zipInputStream.getNextEntry();
      byte[] buffer = new byte[1024];
      int len;
      while ((len = zipInputStream.read(buffer)) > 0) {
          unzippedOutputStream.write(buffer, 0, len);
      }

      logger.info("File unzipped");
      return unzippedOutputStream.toByteArray();
    }
  }

  

  private String generateAvroFileName(String baseFileName, String sheetName) {
    String sanitizedSheetName = sheetName.replaceAll("[^a-zA-Z0-9.-]", "_");
    return baseFileName + "_" + sanitizedSheetName + "_" + sheetCounter.getAndIncrement() + ".avro";
  }

  private void uploadToGCS(byte[] content, String bucketName, String fileName) throws IOException {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobId blobId = BlobId.of(bucketName, fileName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
      .setContentType("application/avro")
      .build();
    storage.create(blobInfo, content);
    logger.info("File uploaded to GCS: " + fileName);
  }

}
/* Sample key_mapping and avro_schema 

  private static final Map<String, String> KEY_MAPPING = Map.ofEntries(
    Map.entry("Product", "product"),
    Map.entry("Entity", "entity"),
    Map.entry("Operation", "operation"),
    Map.entry("Campaign ID", "campaign_id"),
    Map.entry("Ad Group ID", "ad_group_id"),
    Map.entry("Portfolio ID", "portfolio_id"),
    Map.entry("Ad ID", "ad_id"),
    Map.entry("Keyword ID", "keyword_id"),
    Map.entry("Product Targeting ID", "product_targeting_id"),
    Map.entry("Campaign Name", "campaign_name"),
    Map.entry("Ad Group Name", "ad_group_name"),
    Map.entry("Campaign Name (Informational only)", "campaign_name_informational"),
    Map.entry("Ad Group Name (Informational only)", "ad_group_name_informational"),
    Map.entry("Portfolio Name (Informational only)", "portfolio_name_informational"),
    Map.entry("Start Date", "start_date"),
    Map.entry("End Date", "end_date"),
    Map.entry("Targeting Type", "targeting_type"),
    Map.entry("State", "state"),
    Map.entry("Campaign State (Informational only)", "campaign_state_informational"),
    Map.entry("Ad Group State (Informational only)", "ad_group_state_informational"),
    Map.entry("Daily Budget", "daily_budget"),
    Map.entry("SKU", "sku"),
    Map.entry("ASIN (Informational only)", "asin_informational"),
    Map.entry("Eligibility Status (Informational only)", "eligibility_status_informational"),
    Map.entry("Reason for Ineligibility (Informational only)", "reason_for_ineligibility_informational"),
    Map.entry("Ad Group Default Bid", "default_bid"),
    Map.entry("Ad Group Default Bid (Informational only)", "default_bid_informational"),
    Map.entry("Bid", "bid"),
    Map.entry("Keyword Text", "keyword_text"),
    Map.entry("Native Language Keyword", "native_language_keyword"),
    Map.entry("Native Language Locale", "native_language_locale"),
    Map.entry("Match Type", "match_type"),
    Map.entry("Bidding Strategy", "bidding_strategy"),
    Map.entry("Placement", "placement"),
    Map.entry("Percentage", "percentage"),
    Map.entry("Product Targeting Expression", "product_targeting_expression"),
    Map.entry("Resolved Product Targeting Expression (Informational only)", "resolved_product_targeting_expression_informational"),
    Map.entry("Impressions", "impressions"),
    Map.entry("Clicks", "clicks"),
    Map.entry("Click-through Rate", "click_through_rate"),
    Map.entry("Spend", "spend"),
    Map.entry("Sales", "sales"),
    Map.entry("Orders", "orders"),
    Map.entry("Units", "units"),
    Map.entry("Conversion Rate", "conversion_rate"),
    Map.entry("ACOS", "acos"),
    Map.entry("CPC", "cpc"),
    Map.entry("ROAS", "roas")
  );

  
  private static String getAvroSchemaJson() {
    return "{\n" +
      "  \"type\": \"record\",\n" +
      "  \"name\": \"SponsoredProductsCampaign\",\n" +
      "  \"fields\": [\n" +
      "    {\"name\": \"product\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"entity\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"operation\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"campaign_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"ad_group_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"portfolio_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"ad_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"keyword_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"product_targeting_id\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"campaign_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"ad_group_name\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"campaign_name_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"ad_group_name_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"portfolio_name_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"start_date\", \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}], \"default\": null},\n" +
      "    {\"name\": \"end_date\", \"type\": [\"null\", {\"type\": \"int\", \"logicalType\": \"date\"}], \"default\": null},\n" +
      "    {\"name\": \"targeting_type\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"state\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"campaign_state_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"ad_group_state_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"daily_budget\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"sku\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"asin_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"eligibility_status_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"reason_for_ineligibility_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"default_bid\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"default_bid_informational\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"bid\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"keyword_text\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"native_language_keyword\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"native_language_locale\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"match_type\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"bidding_strategy\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"placement\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"percentage\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"product_targeting_expression\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"resolved_product_targeting_expression_informational\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
      "    {\"name\": \"impressions\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
      "    {\"name\": \"clicks\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
      "    {\"name\": \"click_through_rate\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"spend\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"sales\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"orders\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
      "    {\"name\": \"units\", \"type\": [\"null\", \"long\"], \"default\": null},\n" +
      "    {\"name\": \"conversion_rate\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"acos\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"cpc\", \"type\": [\"null\", \"double\"], \"default\": null},\n" +
      "    {\"name\": \"roas\", \"type\": [\"null\", \"double\"], \"default\": null}\n" +
      "  ]\n" +
      "}";
  }

*/
