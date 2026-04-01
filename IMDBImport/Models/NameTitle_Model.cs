using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport.Models
{
    public class NameTitle_Model
    {
        public int NConst { get; set; }
        public int TConst { get; set; }

        public NameTitle_Model(string[] nameTitleInfo)
        {
            NConst = int.Parse(nameTitleInfo[0].Substring(2));
            TConst = int.Parse(nameTitleInfo[1].Substring(2));
        }

        public override string ToString()
        {
            return NConst + " - " + TConst;
        }
    }
}
