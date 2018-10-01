using System;
using System.IO;
using AWSConverterController;
using System.Collections.Generic;
using System.Linq;

namespace ControllerTest
{
    class TestProgram
    {
        static void Main(string[] args)
        {
            // test_ConversionRequestMessage_GetMessageBody();
            // test_ConversionResponseMessage_parseMessage();
            // test_ConversionResponseMessage_AppendNonProcessedIdsToFile();
            // test_ConversionResponseMessage_GetMessageBody();
            test_ConversionResponseMessage_parseMessage_2();
            Console.Read();
        }
        public static void test_ConversionRequestMessage_GetMessageBody()
        {
            ConversionRequestMessage reqmsg = new ConversionRequestMessage();
            reqmsg.AddIdAndFile(1, "test1");
            reqmsg.AddIdAndFile(2, "test2");
            reqmsg.AddIdAndFile(3, "test3");

            string body = reqmsg.GetMessageBody("|");
            Console.WriteLine(body);

            List<Tuple<int, string>> tt = ConversionRequestMessage.GetIdAndFileTuples(body, "|");
            foreach (Tuple<int, string> ttp in tt)
            {
                Console.WriteLine(ttp.Item1.ToString() + " " + ttp.Item2);
            }
        }

        public static void test_ConversionResponseMessage_parseMessage()
        {
            ConversionResponseMessage responseMessageClass = new ConversionResponseMessage();
            responseMessageClass.ParseMessage("12345||1|abc|2|dfg");
            string requestMessageId = responseMessageClass.RequestMessageId;
            Console.WriteLine(requestMessageId);
        }

        public static void test_ConversionResponseMessage_parseMessage_2()
        {
            ConversionResponseMessage responseMessageClass = new ConversionResponseMessage();
            responseMessageClass.ParseMessage("12345|");
            string requestMessageId = responseMessageClass.RequestMessageId;
            Console.WriteLine(requestMessageId);
        }

        public static void test_ConversionResponseMessage_AppendNonProcessedIdsToFile()
        {
            ConversionResponseMessage responseMessageClass = new ConversionResponseMessage();
            responseMessageClass.ParseMessage("12345||1|abc|2|dfg");
            StreamWriter sw = new System.IO.StreamWriter(@"C:\transfer\solid-conversions\test-non-processed.txt", true);
            responseMessageClass.AppendNonProcessedIdsToFile(sw);
            sw.Close();
        }

        public static void test_ConversionResponseMessage_GetMessageBody()
        {
            ConversionResponseMessage reqmsg = new ConversionResponseMessage("123456676");
            reqmsg.AddIdAndFileUrlThatIsNotProcessed(1, "test1");
            reqmsg.AddIdAndFileUrlThatIsNotProcessed(2, "test2");
            reqmsg.AddIdAndFileUrlThatIsNotProcessed(3, "test3");

            string body = reqmsg.GetMessageBody();
            Console.WriteLine(body);

        }
    }
}