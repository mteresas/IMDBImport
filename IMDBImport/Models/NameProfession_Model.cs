using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport.Models
{
    public class NameProfession_Model
    {
        public int NConst { get; set; }
        public string PrimaryProfession { get; set; }

        public NameProfession_Model(string[] nameProfessionInfo)
        {
            NConst = int.Parse(nameProfessionInfo[0].Substring(2));
            PrimaryProfession = nameProfessionInfo[1];
        }

        public override string ToString()
        {
            return NConst + " - " + PrimaryProfession;
        }
    }
}
