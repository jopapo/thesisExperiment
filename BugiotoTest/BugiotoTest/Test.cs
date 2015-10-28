using CassandraSharp;
using CassandraSharp.Config;
using CassandraSharp.CQLPoco;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BugiotoTest
{
    public class Test
    {

        public void Run()
        {
            XmlConfigurator.Configure();
            using (ICluster cluster = ClusterManager.GetCluster("AWS_VPC_SA_EAST_1"))
            {
                new Keyspace(cluster).Prepare();


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
        }

    }
}
