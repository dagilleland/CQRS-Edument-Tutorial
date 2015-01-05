using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CafeApp.Events
{
    public class TabOpened
    {
        public Guid Id;
        public int TableNumber;
        public string Waiter;
    }
}
