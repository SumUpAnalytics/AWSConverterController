using System;
using System.Collections.Generic;
using NLog;

/**
 * this calss is to be used to collect data about instancess that are processing 
 * conversion request
 */
namespace AWSConverterController
{
    class InstanceCollector
    {
        private Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public int TotalMessages { get; set; } 

        IDictionary<string, DateTime> InstanceLastResponse = new Dictionary<string, DateTime>();
        IDictionary<string, int> InstanceMessageCount = new Dictionary<string, int>();
        // default constructor 
        public InstanceCollector()
        {
            
        }

        public void collectResponseTime( string instanceId)
        {
            this.InstanceLastResponse[instanceId] = DateTime.Now;
            if( this.InstanceMessageCount.ContainsKey(instanceId))
            {
                this.InstanceMessageCount[instanceId] = this.InstanceMessageCount[instanceId] + 1;
            }
            else
            {
                this.InstanceMessageCount[instanceId] = 1;
            }
        }

        public void PrintAllInstances()
        {
            logger.Info("ALL instances");
            int summ = 0;
            foreach ( string instance in this.InstanceLastResponse.Keys)
            {
                logger.Info("{0} : {1}",instance, this.InstanceMessageCount[instance]);
                summ = summ + this.InstanceMessageCount[instance];
            }
            logger.Info("Total messages: {0} processed: {1}", this.TotalMessages, summ);

        }

        public void PrintNonResponsiveInstances( double maxDelayInMinutes)
        {
            logger.Info("Non responsive instances");
            foreach (string instance in this.InstanceLastResponse.Keys)
            {
                double realDelay = DateTime.Now.Subtract(this.InstanceLastResponse[instance]).TotalMinutes;
                if (realDelay > maxDelayInMinutes)
                    logger.Info(instance);
            }
        }
    }
}
