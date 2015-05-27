# FileMonitor
File monitoring system consists of two components. The first one, The Tailer, monitors "n" files in "n" environments and sends the data to the AWS Kinesis. The second, The StreamProcessor, continuously reads the streams where the Tailer sends the data and builds two repositories of data: A consolidated archive logs, and a database of performance analysis data.
