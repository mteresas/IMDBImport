using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport.Models
{
    public class CrewDirector_Model
    {
        public int TConst { get; set; }
        public int NConst { get; set; }

        public CrewDirector_Model(string[] crewDirectorInfo)
        {
            TConst = int.Parse(crewDirectorInfo[0].Substring(2));
            NConst = int.Parse(crewDirectorInfo[1].Substring(2));
        }

        public override string ToString()
        {
            return TConst + " - " + NConst;
        }
    }
}
