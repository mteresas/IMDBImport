using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport.Models
{
    public class CrewWriter_Model
    {
        public int TConst { get; set; }
        public int NConst { get; set; }

        public CrewWriter_Model(string[] crewWriterInfo)
        {
            TConst = int.Parse(crewWriterInfo[0].Substring(2));
            NConst = int.Parse(crewWriterInfo[1].Substring(2));
        }

        public override string ToString()
        {
            return TConst + " - " + NConst;
        }
    }
}
