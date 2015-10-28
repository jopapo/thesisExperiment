using CassandraSharp;
using CassandraSharp.CQLPoco;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BugiotoTest
{
    public class Keyspace
    {
        public void Prepare(ICluster cluster)
        {
            CreateKeyspace<BugiotoModel>(cluster, "BUGIOTO");
            CreateKeyspace<BugiotoModel>(cluster, "POFFO");
        }

        private void CreateKeyspace<T>(ICluster cluster, string keyspaceName)
        {
            var cmd = cluster.CreatePocoCommand();

            const string cqlKeyspaces = "SELECT KeyspaceName from system.schema_keyspaces Where KeyspaceName = ?";

            // async operation with streaming
            cmd.Execute<SchemaKeyspaces>(cqlKeyspaces)
                
               .Subscribe(DisplayKeyspace);

            // future
            var kss = await cmd.Execute<SchemaKeyspaces>(cqlKeyspaces).AsFuture();
            foreach (var ks in kss)
            {
                DisplayKeyspace(ks);
            }


            if (cluster.)
            throw new NotImplementedException();
        }

    }
}
