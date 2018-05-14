using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeepCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            Student a = new Student();
            a.name = "gaofan";
            a.ID = "101";
            a.ranking = 1;
            a.hobby = new Hobby { name = "ball", type = HobbyType.backetball };

            test.Student b = a.DeepCopyTo<test.Student>();

            int x = 0;

            Console.ReadKey();
        }
    }

    class Student {
        public string name;
        public string ID;
        public int ranking;
        public string nickName;
        public Hobby hobby;
    }

    class Hobby {
        public string name;
        public HobbyType type;

    }

    enum HobbyType {
        none = 0,
        backetball = 1,
        football =2
    }
}

namespace test
{
    class Student
    {
        public string name;
        public string ID;
        public int ranking;
        public Hobby hobby;
    }

    class Hobby
    {
        public string name;
        public HobbyType type;

    }

    enum HobbyType
    {
        none = 0,
        backetball = 1,
        football = 2
    }
}
