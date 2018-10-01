using System;
using System.IO;
using System.Collections.Generic;

using MySql.Data.MySqlClient;
using Amazon.SQS;
using Amazon.SQS.Model;
using NLog;
using BankDataDynamoDbDAO;

namespace AWSConverterController
{
    class Program
    {
        static Logger logger = NLog.LogManager.GetCurrentClassLogger();
        static void Main(string[] args)
        {
            // read configuration
            
            string toDebug = System.Configuration.ConfigurationManager.AppSettings["toDebug"];
            string idFileName = System.Configuration.ConfigurationManager.AppSettings["idFileName"];

            string notProcessedIdsFileName = System.Configuration.ConfigurationManager.AppSettings["notProcessedIdsFileName"];
            
            // "server=liveboard.cjvgiw4swlyc.us-west-1.rds.amazonaws.com;database=sum_up;uid=yerem;pwd=sua.liveboard.2018;";
            string connectionString = System.Configuration.ConfigurationManager.AppSettings["connectionString"];

            string pdfSource = System.Configuration.ConfigurationManager.AppSettings["pdfSource"];

            // 5 
            string rowsToProcessAsStr = System.Configuration.ConfigurationManager.AppSettings["rowsToProcessPerMessage"];
            int rowsToProcess = 10;
            if (!Int32.TryParse(rowsToProcessAsStr, out rowsToProcess))
            {
                logger.Fatal("Error conveting rowsToProces to int " + rowsToProcessAsStr);
                System.Environment.Exit(1);
            }
            string numberOfMessagesToSendAsStr = System.Configuration.ConfigurationManager.AppSettings["numberOfMessagesToSend"];
            int numberOfMessagesToSend = 5;
            if (!Int32.TryParse(numberOfMessagesToSendAsStr, out numberOfMessagesToSend))
            {
                logger.Fatal ("Error conveting numberOfMessagesToSend to int " + numberOfMessagesToSendAsStr);
                System.Environment.Exit(1);
            }

            string sendingQueueUrl = System.Configuration.ConfigurationManager.AppSettings["sendingQueueUrl"];
            string receivingQueueUrl = System.Configuration.ConfigurationManager.AppSettings["receivingQueueUrl"];

            // sleep time between checking for messages
            int sleepTimeMillis = 10 * 1000;

            // max delay between two responses so every response within collector has to be < 40 min
            
            double maxDelayInMinutes = 40.0;
            // print configuration parameters 

            // skip processed ids
            string skipProcessedIdAsString = System.Configuration.ConfigurationManager.AppSettings["skipProcessedId"];
            bool skipProcessedId = skipProcessedIdAsString.Equals("T");

            if (toDebug == "Y")
            {
                Console.WriteLine("Input parameters");

                Console.WriteLine(idFileName);
                Console.WriteLine(notProcessedIdsFileName);

                Console.WriteLine(connectionString);
                Console.WriteLine(pdfSource);
                Console.WriteLine(rowsToProcessAsStr);
                Console.WriteLine(numberOfMessagesToSendAsStr);

                Console.WriteLine(sendingQueueUrl);
                Console.WriteLine(skipProcessedId);
            }

            // open stream writer for non processes files 
            StreamWriter notProcessedStream = new StreamWriter(notProcessedIdsFileName, true);


            // get last processed id 

            // int lastId = GetLastProcessedId(idFileName);
            // new version 
            // establish connection to dynamo db table to exclude already processed rows
            BankDataProcessingDynamoDbDAO bankDataProcessing =
                new BankDataProcessingDynamoDbDAO("us-west-2");
            bool isOk = bankDataProcessing.Connect();
            if (!isOk)
            {
                logger.Fatal("Can not connect to dynamo db");
                System.Environment.Exit(1);
            }

            int lastId = bankDataProcessing.GetMaxIdForSource(pdfSource);

            Console.WriteLine("last:" + lastId.ToString());

            // connect to sending queue
            AmazonSQSConfig sqsConfig = new AmazonSQSConfig();
            // sqsConfig.ServiceURL = sendingQueueUrl;
            // this is needed in order to avoid error message 
            // The specified queue does not exist for this wsdl version
            sqsConfig.RegionEndpoint = Amazon.RegionEndpoint.USWest2;

            AmazonSQSClient sqsClient = new AmazonSQSClient(sqsConfig);
            int imsg = 0;
            char delimiter = '|';
            // keep messages in memory for possible problem handling
            Dictionary<string, ConversionRequestMessage> messagesTable = new Dictionary<string, ConversionRequestMessage>();
            Dictionary<string, bool> isMessageProcessed = new Dictionary<string, bool>();
            // loop over predefined number of messages
            bool allSent = false;
            bool allReceived = false;
            int lastProcessedId = lastId;

            ConversionResponseMessage responseMessageClass = new ConversionResponseMessage();
            InstanceCollector instanceCollectior = new InstanceCollector();

            while ( ! ( allSent && allReceived) ) 
            {
                // loop over predefined number of messages
                // send message until the limit 
                if (! allSent )
                {
                    // read from the database
                    // open and close connection to avoid long running connections

                    MySqlConnection conn = GetConnection(connectionString);
                    if (conn == null)
                    {
                        logger.Error("Error connecting to databse");
                        System.Environment.Exit(1);
                    }

                    ConversionRequestMessage msg = ConstructMessage(lastId, rowsToProcess, pdfSource, delimiter, conn, bankDataProcessing, skipProcessedId );
                    conn.Close();

                    if (msg.Size() > 0)
                    {
                        // prepare message
                        Console.WriteLine(msg.GetMessageBody(delimiter.ToString()));
                        // send message 
                        imsg++;
                        logger.Debug("sending message: " + imsg.ToString());
                        SendMessageRequest request = new SendMessageRequest();
                        request.MessageBody = msg.GetMessageBody(delimiter.ToString());
                        request.QueueUrl = sendingQueueUrl;
                        SendMessageResponse response = sqsClient.SendMessage(request);
                        string messageId = response.MessageId;
                        logger.Info("message sent: " + messageId);

                        messagesTable.Add(messageId, msg);
                        isMessageProcessed.Add(messageId, false);
                        lastId = msg.getMaxId();
                        if( imsg == numberOfMessagesToSend)
                        {
                            allSent = true;
                        }
                    }
                    else
                    {
                        // no more data to send 
                        allSent = true;
                    }
                }
                else
                {
                    bankDataProcessing.Disconnect();
                }
                                    
                if( ! allReceived )
                {
                    ReceiveMessageRequest recRequest = new ReceiveMessageRequest();
                    recRequest.QueueUrl = receivingQueueUrl;
                    recRequest.MaxNumberOfMessages = 5;
                    logger.Debug("checking message from receiving queue ");
                    ReceiveMessageResponse response = sqsClient.ReceiveMessage(recRequest);
                    if (response.Messages.Count > 0)
                    {
                        foreach (var message in response.Messages)
                        {
                            string messageReceiptHandle = message.ReceiptHandle;
                            responseMessageClass.ParseMessage(message.Body);
                            string requestMessageId = responseMessageClass.RequestMessageId;
                            responseMessageClass.AppendNonProcessedIdsToFile(notProcessedStream);

                            // collect instance id and response time 
                            instanceCollectior.collectResponseTime(responseMessageClass.InstanceId);
                 
                            logger.Info("processing confirmation message for : " + requestMessageId);
                            
                            // flag message as processed
                            if ( isMessageProcessed.ContainsKey(requestMessageId))
                            {
                                isMessageProcessed[requestMessageId] = true;
                                if (messagesTable[requestMessageId].getMaxId() > lastProcessedId)
                                {
                                    lastProcessedId = messagesTable[requestMessageId].getMaxId();
                                    SaveLastProcessedId(idFileName, lastProcessedId);
                                }
                            }
                            else
                            {
                                logger.Warn("Inconsistent messager id" + requestMessageId);
                            }
                            // delete processed message 
                            
                            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
                            deleteMessageRequest.QueueUrl = receivingQueueUrl;
                            deleteMessageRequest.ReceiptHandle = messageReceiptHandle;

                            // success is not tested !!!
                            sqsClient.DeleteMessage(deleteMessageRequest);
                        }


                        allReceived = verifyThatAllMessagesAreReceived(isMessageProcessed, imsg);
                        // todo: deal with situation when all messages are send but not all are received and there is a timeout !!!

                    }
                    // sleep a little before you go for next response message
                    if ( allSent )
                        System.Threading.Thread.Sleep(sleepTimeMillis);

                }
                instanceCollectior.TotalMessages = imsg;
                instanceCollectior.PrintAllInstances();
                instanceCollectior.PrintNonResponsiveInstances(maxDelayInMinutes);
            }

            notProcessedStream.Close();

            logger.Info("Done");
            // Console.Read();
        }
        /*
         * this one will verify that every message has beren processed and that number of send messages is actually 
         * equal to number of messages sent 
         */
        public static bool verifyThatAllMessagesAreReceived(Dictionary<string, bool> isMessageProcessed, int noMsg)
        {
            
            if( isMessageProcessed.Count != noMsg )
                return false;
            bool ret = true;
            foreach ( bool value in isMessageProcessed.Values)
            {
                if( ! value )
                {
                    ret = false;
                    break;
                }
            }
            return ret;
        } 
        /*
         * this one will prepare some sort of message class that keeps content in certain format 
         * message body will be send over SQS
         */
        public static ConversionRequestMessage ConstructMessage(int lastId, int rowcount, string pdfSource, 
            char delimiter, MySqlConnection conn, BankDataProcessingDynamoDbDAO bankDataProcessing, bool skipProcessedId )
        {
            string query = "select id, file_url from bank_data " +
                "where id > @p1 " +
                "and source = @p2 " +
                "and file_url like '%.pdf' " +
                "limit @p3; ";
            MySqlCommand cmd = new MySqlCommand(query, conn);
            cmd.Parameters.AddWithValue("@p1", lastId);
            cmd.Parameters.AddWithValue("@p2", pdfSource);
            cmd.Parameters.AddWithValue("@p3", rowcount);

            logger.Debug(cmd.CommandText);
            MySqlDataReader reader = cmd.ExecuteReader();

            ConversionRequestMessage msg = new ConversionRequestMessage();

            while (reader.Read())
            {
                int id = reader.GetInt32(0);
                // this part will skip ids that are already in dynamo db table 
                // they will not be re-processed !!!!
                if (skipProcessedId)
                {
                    if (bankDataProcessing.IsIdPresent(id))
                        continue;
                }
                string fileUrl = reader.GetString(1);
                msg.AddIdAndFile(id, fileUrl);    

            }
            reader.Close();

            return msg;
        }
        /*
 * this one will read text file that contains id of the last processed file, from the database table
 */

        public static int GetLastProcessedId(string idFileName)
        {
            int lastId = 0;
            try
            {   // Open the text file using a stream reader.
                using (StreamReader sr = new StreamReader(idFileName))
                {
                    String line = sr.ReadLine();
                    logger.Debug("line:" + line);
                    if (Int32.TryParse(line, out lastId))
                    {
                        logger.Info("Last id found:" + lastId);
                    }
                    else
                        logger.Warn("Last id file record has problem.");
                }

            }
            catch (Exception e)
            {
                logger.Warn("can not read last id file: " + idFileName);
                logger.Warn(e.Message);
            }

            return lastId;
        }

        /*
         * saves id in the file for the future
         * not fastest solution 
         */
        public static void SaveLastProcessedId(string idFileName, int lastId)
        {
            try
            {   // Open the text file using a stream reader.
                using (StreamWriter sw = new StreamWriter(idFileName))
                {
                    // Read the stream to a string, and write the string to the console.
                    sw.WriteLine(lastId);
                }

            }
            catch (Exception e)
            {
                logger.Error("can not Write last id file: " + idFileName);
                logger.Error(e.Message);
            }
        }

        /*
         * get sql connection
         */
        public static MySqlConnection GetConnection(string connectionString)
        {
            MySqlConnection cnn = null;
            try
            {
                cnn = new MySqlConnection(connectionString);
                cnn.Open();
                logger.Debug("Connection established ");
            }
            catch (Exception ex)
            {
                logger.Error("Can not open sql connection ! ");
                logger.Error(ex.Message);
            }
            return cnn;
        }
    }
}
