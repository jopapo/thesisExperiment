using CassandraSharp;
using CassandraSharp.Config;
using CassandraSharp.CQLPoco;
using System;
using System.Threading.Tasks;

namespace BugiotoTest
{
    public class SchemaKeyspaces
    {
        public bool DurableWrites { get; set; }

        public string KeyspaceName { get; set; }

        public string StrategyClass { get; set; }

        public string StrategyOptions { get; set; }
    }

    public class Sample
    {
        private void DisplayKeyspace(SchemaKeyspaces ks)
        {
            Console.WriteLine("DurableWrites={0} KeyspaceName={1} strategy_Class={2} strategy_options={3}",
                              ks.DurableWrites,
                              ks.KeyspaceName,
                              ks.StrategyClass,
                              ks.StrategyOptions);
        }

        public async Task QueryKeyspaces()
        {
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("AWS_VPC_SA_EAST_1"))
            {
                var cmd = cluster.CreatePocoCommand();

                const string cqlKeyspaces = "SELECT * from system.schema_keyspaces";

                // async operation with streaming
                cmd.WithConsistencyLevel(ConsistencyLevel.ONE)
                   .Execute<SchemaKeyspaces>(cqlKeyspaces)
                   .Subscribe(DisplayKeyspace);

                // future
                var kss = await cmd.Execute<SchemaKeyspaces>(cqlKeyspaces).AsFuture();
                foreach (var ks in kss)
                {
                    DisplayKeyspace(ks);
                }
            }

            ClusterManager.Shutdown();
        }
    }

}
