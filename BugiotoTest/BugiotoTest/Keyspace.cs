using CassandraSharp;
using CassandraSharp.CQLOrdinal;
using System.Collections.Generic;

namespace BugiotoTest
{
    public class Keyspace
    {
        public const string POF = "iceis_poffo";
        public const string BUG = "iceis_bugioto";

        public void Prepare(ICluster cluster)
        {
            CreateBugiotoKeyspace(cluster);
            CreatePoffoKeyspace(cluster);
        }

        private void CreatePoffoKeyspace(ICluster cluster)
        {
            var list = new List<string>() {
                "drop keyspace if exists {0}",
                "create keyspace {0} with replication = {{'class' : 'SimpleStrategy', 'replication_factor' : 3}}",
                "create table {0}.Player (userName text primary key, firstName text, lastName text)",
                "create table {0}.GameInfo (userName text, Game text, primary key (userName, Game))",
                "create table {0}.Game (id text primary key, firstPlayer text, secondPlayer text)",
                "create table {0}.GameRound (id text, Round text, primary key (id, Round))",
                "create table {0}.Round (id text primary key, game text, moves list<text>, comments list<text>, actions list<text>, spells list<text>)",
                "insert into {0}.Player (userName, firstName, lastName) values ('player1', 'John', 'Fen')",
                "insert into {0}.Player (userName, firstName, lastName) values ('player2', 'Fran', 'Bug')" };

            var cmd = cluster.CreateOrdinalCommand();
            foreach (var c in list)
                cmd.Execute(string.Format(c, POF)).AsFuture().Wait();
        }

        private void CreateBugiotoKeyspace(ICluster cluster)
        {
            var list = new List<string>() {
                "drop keyspace if exists {0}",
                "create keyspace {0} with replication = {{'class' : 'SimpleStrategy', 'replication_factor' : 3}}",
                //"CREATE TYPE {0}.type_gameinfo (game text, opponent text)",
                "create table {0}.Player (userName text primary key, firstName text, lastName text, gamesGame list<text>, gamesOpponent list<text>)",
                //"CREATE TYPE {0}.type_round (id text, moves list<text>)",
                "create table {0}.Game (id text primary key, firstPlayer text, secondPlayer text, roundsId list<text>, roundsMoves list<text>, roundsComments list<text>, roundsActions list<text>, roundsSpells list<text>)",
                "insert into {0}.Player (userName, firstName, lastName) values ('player1', 'John', 'Fen')",
                "insert into {0}.Player (userName, firstName, lastName) values ('player2', 'Fran', 'Bug')" };

            var cmd = cluster.CreateOrdinalCommand();
            foreach (var c in list)
                cmd.Execute(string.Format(c, BUG)).AsFuture().Wait();
        }

    }
}
