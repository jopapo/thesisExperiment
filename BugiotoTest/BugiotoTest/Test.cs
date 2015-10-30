using CassandraSharp;
using CassandraSharp.Config;
using CassandraSharp.CQLPropertyBag;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace BugiotoTest
{
    public class Test
    {

        private int iteration = 0;

        public void Run()
        {
            if (File.Exists("times.csv"))
            {
                File.Move("times.csv", string.Format("times{0}.csv", DateTime.Now.ToString("_yyyy-MM-dd-HH-mm-ss")));
            }
            File.AppendAllText("times.csv", "id;keystore;operation;seconds;size\n");

            try
            {
                XmlConfigurator.Configure();

                var cluster = ClusterManager.GetCluster("AWS_VPC_SA_EAST_1");

                /*var cluster = Cluster.Builder()
                  .AddContactPoints("54.201.93.184", "52.88.48.116", "54.201.83.65")
                  .WithCredentials("iccassandra", "f8648fdddc68dd1054a519a7c3a373e3")
                  .Build();*/

                //using (var cluster = cluster.Connect())
                using (cluster)
                {
                    new Keyspace().Prepare(cluster);

                    TestIt(cluster);                  
                }
            }
            finally
            {
                ClusterManager.Shutdown();
            }
        }

        public ulong GetSize(ICluster cluster, string name)
        {
            var cql = string.Format("select mean_partition_size, partitions_count from system.size_estimates where keyspace_name='{0}'", name);
            var res = cluster.CreatePropertyBagCommand().WithConsistencyLevel(ConsistencyLevel.ONE).Execute(cql).AsFuture().Result;

            ulong total = 0;
            foreach (var r in res)
            {
                total += Convert.ToUInt64(r["mean_partition_size"]) * Convert.ToUInt64(r["partitions_count"]);
            }

            return total;
        }

        public void Write(ICluster cluster, string keystore, string operation, TimeSpan timeSpan)
        {
            var line = string.Format("{0};{1};{2};{3};{4}\n", iteration, keystore, operation, timeSpan.TotalSeconds, GetSize(cluster, keystore));
            Console.Write("linha: " + line);
            while (true)
            {
                try
                {
                    File.AppendAllText("times.csv", line);
                    break;
                }
                catch (Exception e)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public void TestIt(ICluster cluster)
        {
            var gameId = 0;
            var roundId = 0;

            for (var gameCount = 0; gameCount < 100; gameCount++)
            {
                gameId++;
                iteration++;

                Poffo_GameTest(cluster, gameId);
                Bugioto_GameTest(cluster, gameId);

                for (var roundCount = 1; roundCount < 100 + gameCount * 100; roundCount++)
                {
                    roundId++;
                    iteration++;

                    // Era pra serem quantidas e tamanhos aleatórios, mas achei melhor não para não influenciar os tempos.
                    var moves = TempTextList(10, 5);
                    var comments = TempTextList(5, 100);
                    var actions = TempTextList(50, 20);
                    var spells = TempTextList(20, 10);

                    Poffo_RoundTest(cluster, gameId, roundId, moves, comments, actions, spells);
                    Bugioto_RoundTest(cluster, gameId, roundId, moves, comments, actions, spells);

                }

            }
        }

        private void Poffo_GameTest(ICluster cluster, int gameId)
        {
            var key = Keyspace.POF;

            // Só há select nos jogadores, supondo que haja informação relevante num cenário real. Mas no código não é usado pra nada.
            var cql = string.Format("select * from {0}.Player where userName = '{1}'", key, "player1");
            var timer = Stopwatch.StartNew();
            var player1 = cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Result.First();
            Write(cluster, key, "SELECT_PLAYER_1", timer.Elapsed);

            cql = string.Format("select * from {0}.Player where userName = '{1}'", key, "player2");
            timer = Stopwatch.StartNew();
            var player2 = cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Result.First();
            Write(cluster, key, "SELECT_PLAYER_2", timer.Elapsed);

            // Cria jogo e atualiza o jogadores
            cql = string.Format("begin batch insert into {0}.Game (id, firstPlayer, secondPlayer) values ('{1}','{2}','{3}');" +
                "insert into {0}.GameInfo (userName, game) values ('{2}', '{1}');" +
                "insert into {0}.GameInfo (userName, game) values ('{3}', '{1}');" +
                "apply batch;"
                , key, gameId, "player1", "player2");

            timer = Stopwatch.StartNew();
            cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Wait();
            Write(cluster, key, "GAME_INSERT", timer.Elapsed);
        }

        private void Bugioto_GameTest(ICluster cluster, int gameId)
        {
            var key = Keyspace.BUG;

            var cql = string.Format("select * from {0}.Player where userName = '{1}'", key, "player1");
            var timer = Stopwatch.StartNew();
            var player1 = cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Result.First();
            Write(cluster, key, "SELECT_PLAYER_1", timer.Elapsed);

            cql = string.Format("select * from {0}.Player where userName = '{1}'", key, "player2");
            timer = Stopwatch.StartNew();
            var player2 = cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Result.First();
            Write(cluster, key, "SELECT_PLAYER_2", timer.Elapsed);

            AddToList(player1, "gamesGame", gameId);
            AddToList(player1, "gamesOpponent", "player2");
            AddToList(player2, "gamesGame", gameId);
            AddToList(player2, "gamesOpponent", "player1");

            // Cria jogo e atualiza o jogadores
            cql = string.Format("begin batch insert into {0}.Game (id, firstPlayer, secondPlayer) values ('{1}','{2}','{3}');" +
                "insert into {0}.Player ({4}) values ({5});" +
                "insert into {0}.Player ({6}) values ({7});" +
                "apply batch;"
                , key, gameId, "player1", "player2"
                , string.Join(",", player1.Keys), string.Join(",", from k in player1.Keys select ConvertValue(player1[k]))
                , string.Join(",", player2.Keys), string.Join(",", from k in player2.Keys select ConvertValue(player2[k])));

            timer = Stopwatch.StartNew();
            cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Wait();
            Write(cluster, key, "GAME_INSERT", timer.Elapsed);
        }

        private void Bugioto_RoundTest(ICluster cluster, int gameId, int roundId, IList<string> moves, IList<string> comments, IList<string> actions, IList<string> spells)
        {
            var key = Keyspace.BUG;

            // Atualiza rounds do jogo.
            var cql = string.Format("select * from {0}.Game where id = '{1}'", key, gameId);
            var timer = Stopwatch.StartNew();
            var game = cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Result.First();
            Write(cluster, key, "SELECT_GAME", timer.Elapsed);

            AddToList(game, "roundsId", roundId);
            AddToList(game, "roundsMoves", string.Join("|",moves));
            AddToList(game, "roundsComments", string.Join("|",comments));
            AddToList(game, "roundsActions", string.Join("|",actions));
            AddToList(game, "roundsSpells", string.Join("|",spells));

            // Atualizando o jogo e inserindo o round
            cql = string.Format("insert into {0}.Game ({1}) values ({2})",
                key, string.Join(",", game.Keys),
                string.Join(",", from k in game.Keys select ConvertValue(game[k])));

            timer = Stopwatch.StartNew();
            cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Wait();
            Write(cluster, key, "ROUND_INSERT", timer.Elapsed);
        }

        private void Poffo_RoundTest(ICluster cluster, int gameId, int roundId, IList<string> moves, IList<string> comments, IList<string> actions, IList<string> spells)
        {
            var key = Keyspace.POF;

            // Mesma coisa que acontece com os jogadores. Só pega oo jogo por se tratar de possívelmente ter informação relevante. Mas no código não é usado.
            var cql = string.Format("select * from {0}.Game where id = '{1}'", key, gameId);
            var timer = Stopwatch.StartNew();
            var game = cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Result.First();
            Write(cluster, key, "SELECT_GAME", timer.Elapsed);

            // Atualizando o jogo e inserindo o round
            cql = string.Format("begin batch " +
                "insert into {0}.Round (id,game,moves,comments,actions,spells) values ('{2}','{1}',{3},{4},{5},{6});" +
                "insert into {0}.GameRound(id, Round) values('{1}', '{2}');" +
                "apply batch;",
                key, gameId, roundId, ConvertValue(moves), ConvertValue(comments), ConvertValue(actions), ConvertValue(spells));

            timer = Stopwatch.StartNew();
            cluster.CreatePropertyBagCommand().Execute(cql).AsFuture().Wait();
            Write(cluster, key, "ROUND_INSERT", timer.Elapsed);
        }

        private void AddToList(PropertyBag prop, string field, object value)
        {
            var list = (IList<string>) prop[field];
            if (list == null)
                prop[field] = list = new List<string>();
            list.Add(Convert.ToString(value));
        }

        private object ConvertValue(object value)
        {
            var list = value as IEnumerable<object>;
            if (list != null)
            {
                return "['" + string.Join("','", list) + "']";
            }
            return "'" + value + "'";
        }

        private IList<string> TempTextList(int count, int wordLength)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var random = new Random();

            var list = new List<string>();

            for (var i = 0; i < count; i++)
            {
                var text = new string(Enumerable.Repeat(chars, wordLength)
                  .Select(s => s[random.Next(s.Length)]).ToArray());

                list.Add(text);
            }
            return list;
        }


    }
}
