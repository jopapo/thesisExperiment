using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BugiotoTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Keyspace.Prepare();
            //(new Sample()).QueryKeyspaces().Wait();


            Console.ReadLine();
        }
    }
}
