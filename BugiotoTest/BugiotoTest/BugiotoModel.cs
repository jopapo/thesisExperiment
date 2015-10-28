using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BugiotoTest
{
    public class BugiotoModel
    {
        public Player player { get; set; }
        public Game game { get; set; }
    }

    public class Player
    {
        public string username { get; set; }
        public string firstname { get; set; }
        public string lastname { get; set; }
    }

    public class Game
    {
        public string id { get; set; }
        public string firstPlayer { get; set; }
        public string secondPlayer { get; set; }
        public IList<Round> rounds { get; set; }
    }

    public class Round
    {
        public string id { get; set; }
        public IList<string> moves { get; set; }
        public IList<string> comments { get; set; }
        public IList<string> actions { get; set; }
        public IList<string> spells { get; set; }
    }

}
